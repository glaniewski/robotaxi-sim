from __future__ import annotations

from typing import Any, Literal, Optional

from pydantic import BaseModel, Field

ChargingQueuePolicy = Literal["jit", "fifo"]
ChargingDepotSelection = Literal["fastest", "fastest_balanced"]


# ------------------------------------------------------------------
# Sub-configs
# ------------------------------------------------------------------

class FleetConfig(BaseModel):
    size: int = 50
    battery_kwh: float = Field(default=75.0, gt=0, le=500.0)
    kwh_per_mile: float = 0.20
    soc_initial: float = 0.80
    soc_min: float = 0.20
    soc_charge_start: float = 0.80
    soc_target: float = 0.80
    soc_buffer: float = 0.05


class DepotConfig(BaseModel):
    id: str
    h3_cell: Optional[str] = None   # None → placed at centroid of Austin ODD
    chargers_count: int = 20
    charger_kw: float = 150.0
    site_power_kw: float = 1500.0


class DemandConfig(BaseModel):
    max_wait_time_seconds: float = 600.0
    day_offset_seconds: float = 0.0   # start of time-of-day window (seconds since midnight)
    demand_scale: float = 1.0         # 1.0 = all trips in window, 0.5 = half, 2.0 = double
    demand_flatten: float = 0.0       # 0.0 = raw historical peaks, 1.0 = fully uniform across 24h
    coverage_polygon: Optional[list[list[float]]] = None  # [[lng,lat],...] GeoJSON polygon; None = all Austin
    # When repeat_num_days > 1: tile the same synthetic-day slice (see load_requests_repeated_days).
    repeat_num_days: int = Field(default=1, ge=1)
    duration_minutes_per_day: Optional[float] = Field(
        default=None,
        gt=0,
    )  # template window length per day; None → 1440 when repeat_num_days > 1


class DemandControlConfig(BaseModel):
    flex_pct: float = 0.0
    flex_minutes: float = 10.0
    pool_pct: float = 0.0
    max_detour_pct: float = 0.15
    prebook_pct: float = 0.0
    eta_threshold_minutes: float = 10.0
    prebook_shift_minutes: float = 10.0
    offpeak_shift_pct: float = 0.0


class RepositioningConfig(BaseModel):
    reposition_enabled: bool = True
    reposition_policy_name: str = "demand_score"  # "demand_score" | "coverage_floor"
    reposition_alpha: float = 0.6
    reposition_half_life_minutes: float = 45.0
    reposition_forecast_horizon_minutes: float = 30.0
    max_reposition_travel_minutes: float = 12.0
    max_vehicles_targeting_cell: int = 3
    reposition_min_idle_minutes: float = 2.0
    reposition_top_k_cells: int = 50
    reposition_lambda: float = 0.05     # travel cost weight: utility = score - lambda * travel_min
    demand_seeded_init: bool = False    # if True, use floor+proportional vehicle distribution


class H3Config(BaseModel):
    resolution: int = 8


class EconomicsConfig(BaseModel):
    # Energy
    electricity_cost_per_kwh: float = 0.068         # $/kWh at the meter (Austin Energy SV2)
    demand_charge_per_kw_month: float = 13.56       # $/kW-month peak demand charge (Austin Energy SV2)
    # Per-mile variable
    maintenance_cost_per_mile: float = 0.03         # $/mile (EV fleet avg; Waymo preset uses 0.05)
    # Per-vehicle-per-day fixed
    insurance_cost_per_vehicle_day: float = 4.00    # ~$1,400/yr liability-dominated
    teleops_cost_per_vehicle_day: float = 3.50      # ~1:40 remote-ops ratio industry avg
    cleaning_cost_per_vehicle_day: float = 6.00     # 1-2 cleanings/day
    # Vehicle depreciation
    base_vehicle_cost_usd: float = 22_500.0         # chassis + AV stack before battery (Tesla default)
    battery_cost_per_kwh: float = 100.0             # pack-level cost ~$100/kWh (2025-26 conservative)
    vehicle_cost_usd: float = 30_000.0              # computed: base + battery_kwh × battery_cost_per_kwh
    vehicle_lifespan_years: float = 5.0             # straight-line depreciation period
    # Infrastructure
    cost_per_site_day: float = 250.0                # lease + ops per depot site per day
    # Revenue (unchanged)
    revenue_base: float = 2.50
    revenue_per_mile: float = 1.50
    revenue_per_minute: float = 0.35
    revenue_min_fare: float = 5.00
    pool_discount_pct: float = 0.25


# ------------------------------------------------------------------
# Vehicle presets — only 3 params differ
# ------------------------------------------------------------------

VEHICLE_PRESETS: dict[str, dict[str, float]] = {
    "tesla": {
        "base_vehicle_cost_usd": 22_500.0,   # $30k total at 75 kWh default
        "kwh_per_mile": 0.20,
        "maintenance_cost_per_mile": 0.03,
    },
    "waymo": {
        "base_vehicle_cost_usd": 72_500.0,   # $80k total at 75 kWh default
        "kwh_per_mile": 0.30,
        "maintenance_cost_per_mile": 0.05,
    },
}


# ------------------------------------------------------------------
# Charger tiers — real products, 10-year amortization
# ------------------------------------------------------------------

CHARGER_TIERS: dict[float, dict[str, float]] = {
    11.5: {"cost_per_post": 2_850.0,  "amortized_per_day": 0.78},
    75.0: {"cost_per_post": 31_250.0, "amortized_per_day": 8.56},
    96.0: {"cost_per_post": 40_313.0, "amortized_per_day": 11.04},
    150.0: {"cost_per_post": 62_500.0, "amortized_per_day": 17.12},
}


class DispatchConfig(BaseModel):
    strategy: str = "nearest"                       # "nearest" | "first_feasible"
    first_feasible_threshold_seconds: float = 300.0  # only used when strategy="first_feasible"


# ------------------------------------------------------------------
# Top-level scenario config
# ------------------------------------------------------------------

class ScenarioConfig(BaseModel):
    seed: int = 123
    duration_minutes: float = 360.0
    fleet: FleetConfig = Field(default_factory=FleetConfig)
    depots: list[DepotConfig] = Field(default_factory=lambda: [
        DepotConfig(id="depot_1", chargers_count=20, charger_kw=150.0, site_power_kw=1500.0)
    ])
    demand: DemandConfig = Field(default_factory=DemandConfig)
    demand_control: DemandControlConfig = Field(default_factory=DemandControlConfig)
    repositioning: RepositioningConfig = Field(default_factory=RepositioningConfig)
    dispatch: DispatchConfig = Field(default_factory=DispatchConfig)
    economics: EconomicsConfig = Field(default_factory=EconomicsConfig)
    h3: H3Config = Field(default_factory=H3Config)
    timeseries_bucket_minutes: float = 1.0
    # "jit" = replan via VEHICLE_IDLE if all plugs busy; "fifo" = wait in depot.queue
    charging_queue_policy: ChargingQueuePolicy = "jit"
    # "fastest" = min time-to-plug; "fastest_balanced" = among depots within slack of best, min load
    charging_depot_selection: ChargingDepotSelection = "fastest"
    charging_depot_balance_slack_minutes: float = Field(default=3.0, ge=0)
    # Minimum plug dwell per session (minutes); 0 = legacy (energy-time only).
    min_plug_duration_minutes: float = Field(default=0.0, ge=0)

    def effective_duration_minutes(self) -> float:
        """Simulation horizon (minutes). When ``repeat_num_days`` > 1, uses tiled demand days."""
        if self.demand.repeat_num_days <= 1:
            return float(self.duration_minutes)
        per_day = float(self.demand.duration_minutes_per_day or 1440.0)
        return float(self.demand.repeat_num_days) * per_day


# ------------------------------------------------------------------
# /run
# ------------------------------------------------------------------

class RunRequest(BaseModel):
    scenario: ScenarioConfig


class TimeSeriesBucket(BaseModel):
    t_minutes: float
    idle_count: int
    to_pickup_count: int = 0
    in_trip_count: int
    charging_count: int
    repositioning_count: int
    pending_requests: int
    eligible_count: int = 0
    served_cumulative: int
    unserved_cumulative: int
    fleet_mean_soc_pct: float = 0.0


class Metrics(BaseModel):
    # Service
    p10_wait_min: float
    median_wait_min: float
    p90_wait_min: float
    served_pct: float
    unserved_count: int
    served_count: int
    sla_adherence_pct: float
    # Fleet
    trips_per_vehicle_per_day: float
    utilization_pct: float
    deadhead_pct: float
    repositioning_pct: float
    avg_dispatch_distance: float
    # Charging
    depot_queue_p90_min: float
    depot_queue_max_concurrent: float
    depot_queue_max_at_site: float
    charger_utilization_pct: float
    charger_utilization_by_depot_pct: dict[str, float]
    depot_arrivals_total: int
    depot_arrivals_by_depot_id: dict[str, int]
    depot_jit_plug_full_total: int
    depot_jit_plug_full_by_depot_id: dict[str, int]
    depot_charge_completions_total: int
    depot_charge_completions_by_depot_id: dict[str, int]
    depot_arrivals_peak_fleet_per_hour: int
    depot_arrivals_peak_max_site_per_hour: int
    depot_charge_completions_peak_fleet_per_hour: int
    depot_charge_completions_peak_max_site_per_hour: int
    charging_session_duration_median_min: float
    charging_session_duration_p90_min: float
    fleet_battery_pct: float
    fleet_soc_median_pct: float
    vehicles_below_soc_target_count: int
    vehicles_below_soc_target_strict_count: int
    total_charge_sessions: int
    # Pooling
    pool_match_pct: float
    # Economics — itemized cost breakdown
    energy_cost: float
    demand_cost: float
    maintenance_cost: float
    fleet_fixed_cost: float
    infra_cost: float
    total_system_cost: float
    total_system_cost_per_trip: float
    system_margin_per_trip: float
    depreciation_per_vehicle_day: float
    # Economics — backward-compatible keys
    cost_per_trip: float                # alias for total_system_cost_per_trip
    cost_per_mile: float
    fixed_cost_total: float             # alias for fleet_fixed_cost
    avg_revenue_per_trip: float
    revenue_total: float
    contribution_margin_per_trip: float
    total_margin: float                 # revenue_total − total_system_cost


class RunResponse(BaseModel):
    metrics: Metrics
    timeseries: list[TimeSeriesBucket]


# ------------------------------------------------------------------
# /compare
# ------------------------------------------------------------------

class ScenarioOverride(BaseModel):
    """Partial override merged on top of a base ScenarioConfig."""
    fleet: Optional[FleetConfig] = None
    depots: Optional[list[DepotConfig]] = None
    demand: Optional[DemandConfig] = None
    demand_control: Optional[DemandControlConfig] = None
    repositioning: Optional[RepositioningConfig] = None
    economics: Optional[EconomicsConfig] = None
    duration_minutes: Optional[float] = None


class ScenarioVariant(BaseModel):
    use_default: bool = True
    base: Optional[ScenarioConfig] = None     # full override
    overrides: Optional[dict[str, Any]] = None  # partial field overrides


class CompareRequest(BaseModel):
    seed: int = 123
    baseline: ScenarioVariant
    variant: ScenarioVariant


class MetricsDelta(BaseModel):
    """Absolute and relative deltas: variant - baseline."""
    p10_wait_min: float
    median_wait_min: float
    p90_wait_min: float
    served_pct: float
    unserved_count: float
    served_count: float
    sla_adherence_pct: float
    trips_per_vehicle_per_day: float
    utilization_pct: float
    deadhead_pct: float
    repositioning_pct: float
    avg_dispatch_distance: float
    depot_queue_p90_min: float
    depot_queue_max_concurrent: float
    depot_queue_max_at_site: float
    charger_utilization_pct: float
    charger_utilization_by_depot_pct: dict[str, float]
    depot_arrivals_total: float
    depot_arrivals_by_depot_id: dict[str, float]
    depot_jit_plug_full_total: float
    depot_jit_plug_full_by_depot_id: dict[str, float]
    depot_charge_completions_total: float
    depot_charge_completions_by_depot_id: dict[str, float]
    depot_arrivals_peak_fleet_per_hour: float
    depot_arrivals_peak_max_site_per_hour: float
    depot_charge_completions_peak_fleet_per_hour: float
    depot_charge_completions_peak_max_site_per_hour: float
    charging_session_duration_median_min: float
    charging_session_duration_p90_min: float
    fleet_battery_pct: float
    fleet_soc_median_pct: float
    vehicles_below_soc_target_count: float
    vehicles_below_soc_target_strict_count: float
    total_charge_sessions: float
    pool_match_pct: float
    energy_cost: float
    demand_cost: float
    maintenance_cost: float
    fleet_fixed_cost: float
    infra_cost: float
    total_system_cost: float
    total_system_cost_per_trip: float
    system_margin_per_trip: float
    depreciation_per_vehicle_day: float
    cost_per_trip: float
    cost_per_mile: float
    fixed_cost_total: float
    avg_revenue_per_trip: float
    revenue_total: float
    contribution_margin_per_trip: float
    total_margin: float


class CompareResponse(BaseModel):
    baseline: Metrics
    variant: Metrics
    deltas: MetricsDelta
    insights: list[str]
