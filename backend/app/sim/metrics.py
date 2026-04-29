from __future__ import annotations

import math
from typing import Any

import numpy as np

from .entities import Depot, Request, RequestStatus, Vehicle, VehicleState


def _below_soc_target_for_metrics(
    v: Vehicle,
    soc_target: float,
    soc_charge_start: float,
    min_plug_duration_minutes: float,
) -> bool:
    """
    True if vehicle counts as "below soc_target" for vehicles_below_soc_target_count.

    When min_plug_duration_minutes > 0, vehicles actively CHARGING with
    SOC already at or above soc_charge_start are treated as operationally
    within target (mandatory dwell in progress).
    """
    if v.soc >= soc_target - 1e-9:
        return False
    if (
        min_plug_duration_minutes > 1e-12
        and v.state == VehicleState.CHARGING
        and v.soc + 1e-9 >= soc_charge_start
    ):
        return False
    return True


def _trip_fare(
    req: Request,
    revenue_base: float,
    revenue_per_mile: float,
    revenue_per_minute: float,
    revenue_min_fare: float,
    pool_discount_pct: float,
) -> float:
    """
    Compute the fare for a single served request using the base+per_mile+per_minute model.
    Pool-matched riders receive a discount on their gross fare.
    Always uses the rider's direct O-D route, not the actual vehicle path.
    """
    miles = req.trip_miles_direct or 0.0
    minutes = (req.trip_duration_seconds or 0.0) / 60.0
    gross = max(
        revenue_min_fare,
        revenue_base + revenue_per_mile * miles + revenue_per_minute * minutes,
    )
    return gross * (1.0 - pool_discount_pct) if req.pool_matched else gross


def _charger_tier_cost_per_day(charger_kw: float) -> float:
    """Amortized daily plug cost from the charger tier table (10-year straight-line)."""
    from ..schemas import CHARGER_TIERS
    tier = CHARGER_TIERS.get(charger_kw)
    if tier:
        return tier["amortized_per_day"]
    kws = sorted(CHARGER_TIERS.keys())
    closest = min(kws, key=lambda k: abs(k - charger_kw))
    return CHARGER_TIERS[closest]["amortized_per_day"]


def compute_metrics(
    vehicles: dict[str, Vehicle],
    requests: dict[str, Request],
    depots: list[Depot],
    duration_s: float,
    # Itemized cost params
    electricity_cost_per_kwh: float = 0.068,
    demand_charge_per_kw_month: float = 13.56,
    maintenance_cost_per_mile: float = 0.03,
    insurance_cost_per_vehicle_day: float = 4.00,
    teleops_cost_per_vehicle_day: float = 3.50,
    cleaning_cost_per_vehicle_day: float = 6.00,
    vehicle_cost_usd: float = 30_000.0,
    vehicle_lifespan_years: float = 5.0,
    cost_per_site_day: float = 250.0,
    kwh_per_mile: float = 0.20,
    # Revenue
    revenue_base: float = 2.50,
    revenue_per_mile: float = 1.50,
    revenue_per_minute: float = 0.35,
    revenue_min_fare: float = 5.00,
    pool_discount_pct: float = 0.25,
    # SOC / charging
    soc_target: float = 0.80,
    soc_charge_start: float = 0.80,
    min_plug_duration_minutes: float = 0.0,
) -> dict[str, Any]:
    """
    Compute all SPEC §16 metrics from final simulation state.

    Callers: `SimulationEngine` applies horizon charging SOC interpolation for
    vehicles still in CHARGING before calling this (SPEC §11).
    """
    req_list = list(requests.values())
    served = [r for r in req_list if r.status == RequestStatus.SERVED]
    unserved = [r for r in req_list if r.status == RequestStatus.UNSERVED]
    total = len(req_list)

    # --- Service ---
    wait_times = [r.actual_wait_seconds / 60.0 for r in served if r.actual_wait_seconds is not None]
    p10_wait = float(np.percentile(wait_times, 10)) if wait_times else 0.0
    median_wait = float(np.median(wait_times)) if wait_times else 0.0
    p90_wait = float(np.percentile(wait_times, 90)) if wait_times else 0.0
    served_pct = len(served) / total * 100.0 if total > 0 else 0.0

    sla_adherent = [
        r for r in served
        if r.actual_wait_seconds is not None
        and r.actual_wait_seconds <= r.max_wait_time_seconds
    ]
    sla_adherence_pct = len(sla_adherent) / total * 100.0 if total > 0 else 0.0

    # --- Fleet ---
    duration_hours = duration_s / 3600.0
    duration_days = duration_hours / 24.0
    n_vehicles = len(vehicles)

    total_trip_miles = sum(v.trip_miles for v in vehicles.values())
    total_pickup_miles = sum(v.pickup_miles for v in vehicles.values())
    total_reposition_miles = sum(v.reposition_miles for v in vehicles.values())
    total_miles = total_trip_miles + total_pickup_miles + total_reposition_miles

    trips_per_vehicle_per_day = (
        len(served) / n_vehicles / duration_days if n_vehicles > 0 and duration_days > 0 else 0.0
    )

    # Miles-based routing quality (legacy — fraction of driven miles that were revenue miles)
    utilization_pct = total_trip_miles / total_miles * 100.0 if total_miles > 0 else 0.0

    deadhead_pct = (
        (total_pickup_miles + total_reposition_miles) / total_miles * 100.0
        if total_miles > 0
        else 0.0
    )
    repositioning_pct = (
        total_reposition_miles / total_miles * 100.0 if total_miles > 0 else 0.0
    )

    # Time-based active utilization: fraction of clock time vehicles are NOT idle.
    # Covers IN_TRIP + TO_PICKUP + REPOSITIONING + TO_DEPOT + CHARGING.
    # Idle vehicles accumulate no miles so they vanish from the miles metrics above;
    # this metric captures them explicitly.
    total_idle_s = sum(v.time_idle_s for v in vehicles.values())
    total_vehicle_time_s = n_vehicles * duration_s if n_vehicles > 0 else 0.0
    active_time_pct = (
        (total_vehicle_time_s - total_idle_s) / total_vehicle_time_s * 100.0
        if total_vehicle_time_s > 0
        else 0.0
    )

    avg_dispatch_distance = (
        total_pickup_miles / len(served) if served else 0.0
    )

    # --- Charging ---
    # Depot queue wait p90: tracked separately during simulation via depot_queue_waits
    # passed in as optional; default 0 if not available
    depot_queue_p90_min = 0.0  # populated by engine if queue_wait_times provided
    charger_utilization_pct = 0.0

    total_charger_capacity_s = sum(d.chargers_count for d in depots) * duration_s
    # Rough charger utilization: sessions * avg_charge_duration / capacity
    # Actual value requires tracking charge duration; set here to sentinel
    charger_utilization_pct = 0.0  # engine will override with actual value

    # --- Pooling ---
    pool_eligible = [r for r in req_list if r.pooled_allowed]
    pool_matched = [r for r in req_list if r.pool_matched]
    pool_match_pct = (
        len(pool_matched) / len(pool_eligible) * 100.0 if pool_eligible else 0.0
    )

    # --- Economics (itemized) ---
    sim_days = duration_s / 86400.0
    n_vehicles = len(vehicles)

    energy_cost = total_miles * kwh_per_mile * electricity_cost_per_kwh

    total_installed_kw = sum(d.chargers_count * d.charger_kw for d in depots)
    demand_cost = total_installed_kw * demand_charge_per_kw_month * (sim_days / 30.0)

    maintenance_cost = total_miles * maintenance_cost_per_mile

    depreciation_per_vehicle_day = vehicle_cost_usd / (vehicle_lifespan_years * 365.0)
    per_vehicle_day = (
        depreciation_per_vehicle_day
        + insurance_cost_per_vehicle_day
        + teleops_cost_per_vehicle_day
        + cleaning_cost_per_vehicle_day
    )
    fleet_fixed_cost = n_vehicles * sim_days * per_vehicle_day

    n_sites = len(depots)
    total_plugs = sum(d.chargers_count for d in depots)
    plug_costs_per_day = sum(
        d.chargers_count * _charger_tier_cost_per_day(d.charger_kw) for d in depots
    )
    infra_cost = sim_days * (n_sites * cost_per_site_day + plug_costs_per_day)

    total_system_cost = energy_cost + demand_cost + maintenance_cost + fleet_fixed_cost + infra_cost
    total_system_cost_per_trip = total_system_cost / len(served) if served else 0.0
    cost_per_mile_val = total_system_cost / total_miles if total_miles > 0 else 0.0

    fares = [
        _trip_fare(r, revenue_base, revenue_per_mile, revenue_per_minute,
                   revenue_min_fare, pool_discount_pct)
        for r in served
    ]
    revenue_total = sum(fares)
    avg_revenue_per_trip = revenue_total / len(served) if served else 0.0
    system_margin_per_trip = avg_revenue_per_trip - total_system_cost_per_trip
    contribution_margin = system_margin_per_trip

    vlist = list(vehicles.values())
    if vlist:
        socs = [v.soc for v in vlist]
        fleet_battery_pct = round(100.0 * float(np.mean(socs)), 2)
        fleet_soc_median_pct = round(100.0 * float(np.median(socs)), 2)
        vehicles_below_soc_target_strict_count = int(
            sum(1 for v in vlist if v.soc < soc_target - 1e-9)
        )
        vehicles_below_soc_target_count = int(
            sum(
                1
                for v in vlist
                if _below_soc_target_for_metrics(
                    v, soc_target, soc_charge_start, min_plug_duration_minutes
                )
            )
        )
        total_charge_sessions = int(sum(v.charge_sessions for v in vlist))
    else:
        fleet_battery_pct = 0.0
        fleet_soc_median_pct = 0.0
        vehicles_below_soc_target_count = 0
        vehicles_below_soc_target_strict_count = 0
        total_charge_sessions = 0

    return {
        # Service (p50 rider wait = median; same distribution as p10/p90)
        "p10_wait_min": round(p10_wait, 3),
        "median_wait_min": round(median_wait, 3),
        "p90_wait_min": round(p90_wait, 3),
        "served_pct": round(served_pct, 2),
        "unserved_count": len(unserved),
        "served_count": len(served),
        "sla_adherence_pct": round(sla_adherence_pct, 2),
        # Fleet
        "trips_per_vehicle_per_day": round(trips_per_vehicle_per_day, 2),
        "utilization_pct": round(utilization_pct, 2),
        "active_time_pct": round(active_time_pct, 2),
        "deadhead_pct": round(deadhead_pct, 2),
        "repositioning_pct": round(repositioning_pct, 2),
        "avg_dispatch_distance": round(avg_dispatch_distance, 3),
        # Charging
        "depot_queue_p90_min": round(depot_queue_p90_min, 3),
        "charger_utilization_pct": round(charger_utilization_pct, 2),
        # Per-depot plug utilization; engine overwrites with scheduled session sums
        "charger_utilization_by_depot_pct": {d.id: 0.0 for d in depots},
        # Depot throughput (engine overwrites when SimulationEngine runs)
        "depot_arrivals_total": 0,
        "depot_arrivals_by_depot_id": {d.id: 0 for d in depots},
        "depot_jit_plug_full_total": 0,
        "depot_jit_plug_full_by_depot_id": {d.id: 0 for d in depots},
        "depot_charge_completions_total": 0,
        "depot_charge_completions_by_depot_id": {d.id: 0 for d in depots},
        "depot_arrivals_peak_fleet_per_hour": 0,
        "depot_arrivals_peak_max_site_per_hour": 0,
        "depot_charge_completions_peak_fleet_per_hour": 0,
        "depot_charge_completions_peak_max_site_per_hour": 0,
        "charging_session_duration_median_min": 0.0,
        "charging_session_duration_p90_min": 0.0,
        "fleet_battery_pct": fleet_battery_pct,
        "fleet_soc_median_pct": fleet_soc_median_pct,
        "vehicles_below_soc_target_count": vehicles_below_soc_target_count,
        "vehicles_below_soc_target_strict_count": vehicles_below_soc_target_strict_count,
        "total_charge_sessions": total_charge_sessions,
        # Pooling
        "pool_match_pct": round(pool_match_pct, 2),
        # Economics — itemized breakdown
        "energy_cost": round(energy_cost, 2),
        "demand_cost": round(demand_cost, 2),
        "maintenance_cost": round(maintenance_cost, 2),
        "fleet_fixed_cost": round(fleet_fixed_cost, 2),
        "infra_cost": round(infra_cost, 2),
        "total_system_cost": round(total_system_cost, 2),
        "total_system_cost_per_trip": round(total_system_cost_per_trip, 4),
        "system_margin_per_trip": round(system_margin_per_trip, 4),
        "depreciation_per_vehicle_day": round(depreciation_per_vehicle_day, 4),
        # Backward-compatible aliases
        "cost_per_trip": round(total_system_cost_per_trip, 4),
        "cost_per_mile": round(cost_per_mile_val, 4),
        "fixed_cost_total": round(fleet_fixed_cost, 2),
        "avg_revenue_per_trip": round(avg_revenue_per_trip, 4),
        "revenue_total": round(revenue_total, 2),
        "contribution_margin_per_trip": round(contribution_margin, 4),
        "total_margin": round(revenue_total - total_system_cost, 2),
    }


def summarize_charger_util_by_depot(by_depot_pct: dict[str, float]) -> dict[str, float | int]:
    """
    Roll up charger_utilization_by_depot_pct for logging / sweeps (full map is huge).

    Returns max, unweighted mean, p90 across depots, and count of sites with util > 0.01%.
    """
    vals = list(by_depot_pct.values())
    if not vals:
        return {
            "depot_charger_util_max_pct": 0.0,
            "depot_charger_util_mean_pct": 0.0,
            "depot_charger_util_p90_pct": 0.0,
            "depot_charger_util_nonzero_count": 0,
        }
    arr = np.array(vals, dtype=float)
    nonzero = int(np.sum(arr > 0.01))
    return {
        "depot_charger_util_max_pct": round(float(np.max(arr)), 2),
        "depot_charger_util_mean_pct": round(float(np.mean(arr)), 2),
        "depot_charger_util_p90_pct": round(float(np.percentile(arr, 90)), 2),
        "depot_charger_util_nonzero_count": nonzero,
    }


def compute_timeseries(snapshots: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """
    Pass-through of engine-emitted snapshots.
    Each snapshot: {t_minutes, idle_count, to_pickup_count, in_trip_count, charging_count,
                    repositioning_count, pending_requests, eligible_count,
                    served_cumulative, unserved_cumulative, fleet_mean_soc_pct}
    """
    return snapshots
