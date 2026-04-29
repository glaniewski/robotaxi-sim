from __future__ import annotations

import asyncio
import json
import os
import queue as _queue
import sys
import threading
from typing import Any

from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import StreamingResponse

from .schemas import (
    CompareRequest,
    CompareResponse,
    Metrics,
    MetricsDelta,
    RunRequest,
    RunResponse,
    ScenarioConfig,
    ScenarioVariant,
    TimeSeriesBucket,
)
from .sim.demand import apply_demand_control, load_requests, load_requests_repeated_days
from .sim.engine import SimConfig, SimulationEngine, build_vehicles
from .sim.entities import Depot
from .sim.reposition_policies import build_policy
from .sim.routing import RoutingCache

app = FastAPI(
    title="Robotaxi-Sim API",
    description="Discrete-event robotaxi simulator for Austin ODD.",
    version="0.1.0",
)
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["GET", "POST"],
    allow_headers=["*"],
)

# ---------------------------------------------------------------------------
# Paths (overridable via environment)
# ---------------------------------------------------------------------------
_TRAVEL_CACHE_PATH = os.environ.get("TRAVEL_CACHE_PATH", "data/h3_travel_cache.parquet")
_REQUESTS_PATH = os.environ.get("REQUESTS_PATH", "data/requests_austin_h3_r8.parquet")

# Austin ODD default depot H3 cell (downtown/6th St — highest trip volume)
_AUSTIN_CENTER_H3 = "88489e3467fffff"

# Cached default scenario config (loaded once from default_scenario.json)
_DEFAULT_SCENARIO_PATH = os.path.join(os.path.dirname(__file__), "default_scenario.json")
_DEFAULT_SCENARIO: ScenarioConfig | None = None


def _load_default_scenario() -> ScenarioConfig:
    global _DEFAULT_SCENARIO
    if _DEFAULT_SCENARIO is None:
        with open(_DEFAULT_SCENARIO_PATH) as f:
            _DEFAULT_SCENARIO = ScenarioConfig(**json.load(f))
    return _DEFAULT_SCENARIO.model_copy(deep=True)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _resolve_config(variant: ScenarioVariant, seed: int) -> ScenarioConfig:
    """Build a ScenarioConfig from a ScenarioVariant, applying overrides."""
    if variant.base:
        base = variant.base
    elif variant.use_default:
        base = _load_default_scenario()
    else:
        base = ScenarioConfig()
    base.seed = seed

    if variant.overrides:
        merged = base.model_dump()
        for section_key, section_val in variant.overrides.items():
            if section_key in merged and isinstance(merged[section_key], dict) and isinstance(section_val, dict):
                merged[section_key].update(section_val)
            else:
                merged[section_key] = section_val
        base = ScenarioConfig(**merged)

    return base


def _run_scenario(
    config: ScenarioConfig,
    progress_callback: Any = None,
) -> dict[str, Any]:
    """Build and run a single simulation scenario. Returns raw engine output."""
    # Routing cache
    routing = RoutingCache(
        parquet_path=_TRAVEL_CACHE_PATH,
        osrm_url=os.environ.get("OSRM_URL", "http://localhost:5000"),
    )

    # Depots
    depots: list[Depot] = []
    for d_cfg in config.depots:
        h3_cell = d_cfg.h3_cell or _AUSTIN_CENTER_H3
        depots.append(
            Depot(
                id=d_cfg.id,
                h3_cell=h3_cell,
                chargers_count=d_cfg.chargers_count,
                charger_kw=d_cfg.charger_kw,
                site_power_kw=d_cfg.site_power_kw,
            )
        )

    # Vehicles
    depot_cells = [d.h3_cell for d in depots]
    eff_duration = config.effective_duration_minutes()
    if config.demand.repeat_num_days > 1 and abs(config.duration_minutes - eff_duration) > 1e-3:
        print(
            f"[scenario] repeat_num_days={config.demand.repeat_num_days}: "
            f"using effective_duration_minutes={eff_duration:.4f} "
            f"(top-level duration_minutes={config.duration_minutes} ignored for horizon)",
            file=sys.stderr,
        )
    sim_config = SimConfig(
        duration_minutes=eff_duration,
        seed=config.seed,
        fleet_size=config.fleet.size,
        battery_kwh=config.fleet.battery_kwh,
        kwh_per_mile=config.fleet.kwh_per_mile,
        soc_initial=config.fleet.soc_initial,
        soc_min=config.fleet.soc_min,
        soc_charge_start=config.fleet.soc_charge_start,
        soc_target=config.fleet.soc_target,
        soc_buffer=config.fleet.soc_buffer,
        max_wait_time_seconds=config.demand.max_wait_time_seconds,
        electricity_cost_per_kwh=config.economics.electricity_cost_per_kwh,
        demand_charge_per_kw_month=config.economics.demand_charge_per_kw_month,
        maintenance_cost_per_mile=config.economics.maintenance_cost_per_mile,
        insurance_cost_per_vehicle_day=config.economics.insurance_cost_per_vehicle_day,
        teleops_cost_per_vehicle_day=config.economics.teleops_cost_per_vehicle_day,
        cleaning_cost_per_vehicle_day=config.economics.cleaning_cost_per_vehicle_day,
        base_vehicle_cost_usd=config.economics.base_vehicle_cost_usd,
        battery_cost_per_kwh=config.economics.battery_cost_per_kwh,
        vehicle_cost_usd=config.economics.base_vehicle_cost_usd + config.fleet.battery_kwh * config.economics.battery_cost_per_kwh,
        vehicle_lifespan_years=config.economics.vehicle_lifespan_years,
        cost_per_site_day=config.economics.cost_per_site_day,
        revenue_base=config.economics.revenue_base,
        revenue_per_mile=config.economics.revenue_per_mile,
        revenue_per_minute=config.economics.revenue_per_minute,
        revenue_min_fare=config.economics.revenue_min_fare,
        pool_discount_pct=config.economics.pool_discount_pct,
        reposition_enabled=config.repositioning.reposition_enabled,
        reposition_alpha=config.repositioning.reposition_alpha,
        reposition_half_life_minutes=config.repositioning.reposition_half_life_minutes,
        reposition_forecast_horizon_minutes=config.repositioning.reposition_forecast_horizon_minutes,
        max_reposition_travel_minutes=config.repositioning.max_reposition_travel_minutes,
        max_vehicles_targeting_cell=config.repositioning.max_vehicles_targeting_cell,
        reposition_min_idle_minutes=config.repositioning.reposition_min_idle_minutes,
        reposition_top_k_cells=config.repositioning.reposition_top_k_cells,
        reposition_lambda=config.repositioning.reposition_lambda,
        dispatch_strategy=config.dispatch.strategy,
        max_detour_pct=config.demand_control.max_detour_pct if config.demand_control.pool_pct > 0 else 0.0,
        first_feasible_threshold_seconds=config.dispatch.first_feasible_threshold_seconds,
        timeseries_bucket_minutes=config.timeseries_bucket_minutes,
        charging_queue_policy=config.charging_queue_policy,
        charging_depot_selection=config.charging_depot_selection,
        charging_depot_balance_slack_minutes=config.charging_depot_balance_slack_minutes,
        min_plug_duration_minutes=config.min_plug_duration_minutes,
    )
    # Requests
    if not os.path.exists(_REQUESTS_PATH):
        raise HTTPException(
            status_code=503,
            detail=(
                f"Request dataset not found at {_REQUESTS_PATH}. "
                "Run scripts/preprocess_rideaustin_requests.py first."
            ),
        )

    if config.demand.repeat_num_days <= 1:
        requests = load_requests(
            parquet_path=_REQUESTS_PATH,
            duration_minutes=config.duration_minutes,
            day_offset_seconds=config.demand.day_offset_seconds,
            max_wait_time_seconds=config.demand.max_wait_time_seconds,
            demand_scale=config.demand.demand_scale,
            demand_flatten=config.demand.demand_flatten,
            seed=config.seed,
            coverage_polygon=config.demand.coverage_polygon,
        )
    else:
        per_day = float(config.demand.duration_minutes_per_day or 1440.0)
        requests = load_requests_repeated_days(
            parquet_path=_REQUESTS_PATH,
            duration_minutes_per_day=per_day,
            num_days=config.demand.repeat_num_days,
            day_offset_seconds=config.demand.day_offset_seconds,
            max_wait_time_seconds=config.demand.max_wait_time_seconds,
            demand_scale=config.demand.demand_scale,
            demand_flatten=config.demand.demand_flatten,
            seed=config.seed,
            coverage_polygon=config.demand.coverage_polygon,
        )
    requests = apply_demand_control(
        requests,
        flex_pct=config.demand_control.flex_pct,
        flex_minutes=config.demand_control.flex_minutes,
        pool_pct=config.demand_control.pool_pct,
        max_detour_pct=config.demand_control.max_detour_pct,
        prebook_pct=config.demand_control.prebook_pct,
        eta_threshold_minutes=config.demand_control.eta_threshold_minutes,
        prebook_shift_minutes=config.demand_control.prebook_shift_minutes,
        offpeak_shift_pct=config.demand_control.offpeak_shift_pct,
        seed=config.seed,
    )

    # Demand cells — used for coverage_floor policy and demand-seeded init.
    # Build from the full dataset (not just the scaled subset) for stable coverage.
    demand_cells_weights = None  # type: Optional[dict[str, float]]
    if config.repositioning.demand_seeded_init or config.repositioning.reposition_policy_name == "coverage_floor":
        import pandas as pd
        _df = pd.read_parquet(_REQUESTS_PATH, columns=["origin_h3", "destination_h3"])
        _counts = _df["origin_h3"].value_counts()
        demand_cells_weights = _counts.to_dict()

    # Build vehicles — optionally using floor+proportional demand seeding
    vehicles = build_vehicles(
        sim_config,
        depot_cells,
        seed=config.seed,
        demand_cells=demand_cells_weights if config.repositioning.demand_seeded_init else None,
    )

    # Build forecast table and repositioning policy
    reposition_policy = None
    if config.repositioning.reposition_enabled:
        forecast_table = _build_forecast_table(requests, eff_duration)
        demand_cell_set = set(demand_cells_weights.keys()) if demand_cells_weights else None
        reposition_policy = build_policy(
            name=config.repositioning.reposition_policy_name,
            alpha=config.repositioning.reposition_alpha,
            half_life_minutes=config.repositioning.reposition_half_life_minutes,
            forecast_horizon_minutes=config.repositioning.reposition_forecast_horizon_minutes,
            max_reposition_travel_minutes=config.repositioning.max_reposition_travel_minutes,
            max_vehicles_targeting_cell=config.repositioning.max_vehicles_targeting_cell,
            min_idle_minutes=config.repositioning.reposition_min_idle_minutes,
            top_k_cells=config.repositioning.reposition_top_k_cells,
            reposition_lambda=config.repositioning.reposition_lambda,
            forecast_table=forecast_table,
            demand_cells=demand_cell_set,
            max_wait_time_seconds=config.demand.max_wait_time_seconds,
        )

    engine = SimulationEngine(
        config=sim_config,
        vehicles=vehicles,
        requests=requests,
        depots=depots,
        routing=routing,
        reposition_policy=reposition_policy,
        progress_callback=progress_callback,
    )
    result = engine.run()

    # Log cache stats and persist any new OSRM-fetched entries
    stats = routing.cache_stats()
    print(
        f"[cache] hits={stats['cache_hits']:,}  misses={stats['cache_misses']:,}"
        f"  hit_rate={stats['hit_rate_pct']:.1f}%  new_entries={stats['new_entries']:,}"
        f"  cache_size={stats['cache_size']:,}",
        file=sys.stderr,
    )
    if stats["new_entries"] > 0 and os.path.exists(_TRAVEL_CACHE_PATH):
        appended = routing.flush_new_entries(_TRAVEL_CACHE_PATH)
        print(
            f"[cache] Flushed {appended:,} new entries to {_TRAVEL_CACHE_PATH}",
            file=sys.stderr,
        )

    return result


def _build_forecast_table(requests: list, duration_minutes: float) -> dict[str, float]:
    """
    Build {h3_cell: arrivals_per_second} from the loaded request list.
    Used as the forecast signal for the repositioning policy.
    """
    duration_s = duration_minutes * 60.0
    counts: dict[str, int] = {}
    for r in requests:
        counts[r.origin_h3] = counts.get(r.origin_h3, 0) + 1
    if duration_s <= 0:
        return {}
    return {cell: count / duration_s for cell, count in counts.items()}


def _depot_utilization_delta(baseline: dict, variant: dict) -> dict[str, float]:
    """Per-depot absolute delta in utilization percentage points (variant - baseline)."""
    bd = baseline.get("charger_utilization_by_depot_pct") or {}
    vd = variant.get("charger_utilization_by_depot_pct") or {}
    keys = sorted(set(bd.keys()) | set(vd.keys()))
    return {k: round(float(vd.get(k, 0.0)) - float(bd.get(k, 0.0)), 4) for k in keys}


def _depot_int_map_delta(baseline: dict, variant: dict, key: str) -> dict[str, float]:
    """Per-depot absolute delta in counts (variant - baseline)."""
    bd = baseline.get(key) or {}
    vd = variant.get(key) or {}
    keys = sorted(set(bd.keys()) | set(vd.keys()))
    return {k: round(float(vd.get(k, 0)) - float(bd.get(k, 0)), 4) for k in keys}


def _compute_deltas(baseline: dict, variant: dict) -> MetricsDelta:
    b = baseline
    v = variant
    return MetricsDelta(
        p10_wait_min=v["p10_wait_min"] - b["p10_wait_min"],
        median_wait_min=v["median_wait_min"] - b["median_wait_min"],
        p90_wait_min=v["p90_wait_min"] - b["p90_wait_min"],
        served_pct=v["served_pct"] - b["served_pct"],
        unserved_count=v["unserved_count"] - b["unserved_count"],
        served_count=v["served_count"] - b["served_count"],
        sla_adherence_pct=v["sla_adherence_pct"] - b["sla_adherence_pct"],
        trips_per_vehicle_per_day=v["trips_per_vehicle_per_day"] - b["trips_per_vehicle_per_day"],
        utilization_pct=v["utilization_pct"] - b["utilization_pct"],
        deadhead_pct=v["deadhead_pct"] - b["deadhead_pct"],
        repositioning_pct=v["repositioning_pct"] - b["repositioning_pct"],
        avg_dispatch_distance=v["avg_dispatch_distance"] - b["avg_dispatch_distance"],
        depot_queue_p90_min=v["depot_queue_p90_min"] - b["depot_queue_p90_min"],
        depot_queue_max_concurrent=v["depot_queue_max_concurrent"] - b["depot_queue_max_concurrent"],
        depot_queue_max_at_site=v["depot_queue_max_at_site"] - b["depot_queue_max_at_site"],
        charger_utilization_pct=v["charger_utilization_pct"] - b["charger_utilization_pct"],
        charger_utilization_by_depot_pct=_depot_utilization_delta(b, v),
        depot_arrivals_total=float(v["depot_arrivals_total"] - b["depot_arrivals_total"]),
        depot_arrivals_by_depot_id=_depot_int_map_delta(b, v, "depot_arrivals_by_depot_id"),
        depot_jit_plug_full_total=float(v["depot_jit_plug_full_total"] - b["depot_jit_plug_full_total"]),
        depot_jit_plug_full_by_depot_id=_depot_int_map_delta(b, v, "depot_jit_plug_full_by_depot_id"),
        depot_charge_completions_total=float(
            v["depot_charge_completions_total"] - b["depot_charge_completions_total"]
        ),
        depot_charge_completions_by_depot_id=_depot_int_map_delta(
            b, v, "depot_charge_completions_by_depot_id"
        ),
        depot_arrivals_peak_fleet_per_hour=float(
            v["depot_arrivals_peak_fleet_per_hour"] - b["depot_arrivals_peak_fleet_per_hour"]
        ),
        depot_arrivals_peak_max_site_per_hour=float(
            v["depot_arrivals_peak_max_site_per_hour"] - b["depot_arrivals_peak_max_site_per_hour"]
        ),
        depot_charge_completions_peak_fleet_per_hour=float(
            v["depot_charge_completions_peak_fleet_per_hour"]
            - b["depot_charge_completions_peak_fleet_per_hour"]
        ),
        depot_charge_completions_peak_max_site_per_hour=float(
            v["depot_charge_completions_peak_max_site_per_hour"]
            - b["depot_charge_completions_peak_max_site_per_hour"]
        ),
        charging_session_duration_median_min=(
            v["charging_session_duration_median_min"] - b["charging_session_duration_median_min"]
        ),
        charging_session_duration_p90_min=(
            v["charging_session_duration_p90_min"] - b["charging_session_duration_p90_min"]
        ),
        fleet_battery_pct=v["fleet_battery_pct"] - b["fleet_battery_pct"],
        fleet_soc_median_pct=v["fleet_soc_median_pct"] - b["fleet_soc_median_pct"],
        vehicles_below_soc_target_count=v["vehicles_below_soc_target_count"] - b["vehicles_below_soc_target_count"],
        vehicles_below_soc_target_strict_count=(
            v["vehicles_below_soc_target_strict_count"] - b["vehicles_below_soc_target_strict_count"]
        ),
        total_charge_sessions=v["total_charge_sessions"] - b["total_charge_sessions"],
        pool_match_pct=v["pool_match_pct"] - b["pool_match_pct"],
        cost_per_trip=v["cost_per_trip"] - b["cost_per_trip"],
        fixed_cost_total=v["fixed_cost_total"] - b["fixed_cost_total"],
        avg_revenue_per_trip=v["avg_revenue_per_trip"] - b["avg_revenue_per_trip"],
        revenue_total=v["revenue_total"] - b["revenue_total"],
        contribution_margin_per_trip=v["contribution_margin_per_trip"] - b["contribution_margin_per_trip"],
        total_margin=v["total_margin"] - b["total_margin"],
    )


def _generate_insights(baseline: dict, variant: dict, deltas: MetricsDelta) -> list[str]:
    insights: list[str] = []

    if abs(deltas.p90_wait_min) >= 0.5:
        direction = "reduced" if deltas.p90_wait_min < 0 else "increased"
        insights.append(
            f"p90 wait {direction} by {abs(deltas.p90_wait_min):.1f} min "
            f"({baseline['p90_wait_min']:.1f} → {variant['p90_wait_min']:.1f} min)."
        )

    if abs(deltas.served_pct) >= 0.5:
        direction = "improved" if deltas.served_pct > 0 else "worsened"
        insights.append(
            f"Serve rate {direction} by {abs(deltas.served_pct):.1f} pp "
            f"({baseline['served_pct']:.1f}% → {variant['served_pct']:.1f}%)."
        )

    if abs(deltas.repositioning_pct) >= 0.5:
        direction = "added" if deltas.repositioning_pct > 0 else "removed"
        insights.append(
            f"Repositioning {direction} {abs(deltas.repositioning_pct):.1f} pp of total miles "
            f"({baseline['repositioning_pct']:.1f}% → {variant['repositioning_pct']:.1f}%)."
        )

    if abs(deltas.deadhead_pct) >= 0.5:
        direction = "increased" if deltas.deadhead_pct > 0 else "decreased"
        insights.append(
            f"Overall deadhead {direction} by {abs(deltas.deadhead_pct):.1f} pp "
            f"({baseline['deadhead_pct']:.1f}% → {variant['deadhead_pct']:.1f}%)."
        )

    if abs(deltas.contribution_margin_per_trip) >= 0.10:
        direction = "improved" if deltas.contribution_margin_per_trip > 0 else "worsened"
        insights.append(
            f"Contribution margin {direction} by ${abs(deltas.contribution_margin_per_trip):.2f}/trip."
        )

    if abs(deltas.vehicles_below_soc_target_count) >= 5:
        direction = "fewer" if deltas.vehicles_below_soc_target_count < 0 else "more"
        insights.append(
            f"Vehicles below SOC target at horizon: {direction} by "
            f"{abs(deltas.vehicles_below_soc_target_count):.0f} "
            f"({baseline['vehicles_below_soc_target_count']:.0f} → "
            f"{variant['vehicles_below_soc_target_count']:.0f})."
        )

    if not insights:
        insights.append("No significant differences detected between baseline and variant.")

    return insights[:5]


# ---------------------------------------------------------------------------
# Routes
# ---------------------------------------------------------------------------

@app.post("/run", response_model=RunResponse)
def run_scenario(body: ScenarioConfig) -> RunResponse:
    result = _run_scenario(body)
    return RunResponse(
        metrics=Metrics(**result["metrics"]),
        timeseries=[TimeSeriesBucket(**b) for b in result["timeseries"]],
    )


@app.post("/run/stream")
async def run_scenario_stream(body: ScenarioConfig) -> StreamingResponse:
    """
    Same as /run but streams NDJSON progress lines while the sim runs.
    Each line is JSON:
      {"type": "progress", "done": N, "total": M}   — emitted every ~200 resolved trips
      {"type": "result",   "metrics": {...}, "timeseries": [...]}  — final line
      {"type": "error",    "message": "..."}          — on failure
    """
    prog_q: _queue.Queue = _queue.Queue()
    result_box: dict[str, Any] = {}

    def _worker() -> None:
        def _cb(done: int, total: int) -> None:
            prog_q.put({"type": "progress", "done": done, "total": total})
        try:
            result_box["data"] = _run_scenario(body, progress_callback=_cb)
        except Exception as exc:
            result_box["error"] = str(exc)
        finally:
            prog_q.put(None)  # sentinel

    threading.Thread(target=_worker, daemon=True).start()

    loop = asyncio.get_event_loop()

    async def _generate():
        while True:
            msg = await loop.run_in_executor(None, prog_q.get)
            if msg is None:
                break
            yield json.dumps(msg) + "\n"
        if "error" in result_box:
            yield json.dumps({"type": "error", "message": result_box["error"]}) + "\n"
        else:
            d = result_box["data"]
            yield json.dumps({
                "type": "result",
                "metrics": d["metrics"],
                "timeseries": d["timeseries"],
            }) + "\n"

    return StreamingResponse(_generate(), media_type="application/x-ndjson")


@app.post("/compare", response_model=CompareResponse)
def compare_scenarios(body: CompareRequest) -> CompareResponse:
    baseline_config = _resolve_config(body.baseline, seed=body.seed)
    variant_config = _resolve_config(body.variant, seed=body.seed)

    baseline_result = _run_scenario(baseline_config)
    variant_result = _run_scenario(variant_config)

    b_metrics = baseline_result["metrics"]
    v_metrics = variant_result["metrics"]
    deltas = _compute_deltas(b_metrics, v_metrics)
    insights = _generate_insights(b_metrics, v_metrics, deltas)

    return CompareResponse(
        baseline=Metrics(**b_metrics),
        variant=Metrics(**v_metrics),
        deltas=deltas,
        insights=insights,
    )


@app.get("/health")
def health() -> dict[str, str]:
    return {"status": "ok"}
