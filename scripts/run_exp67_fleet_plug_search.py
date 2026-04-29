"""
Experiment 67 — Exp66-class config (N=77, 3-day continuous, Exp63 sim): find fleet + plugs for served% > threshold.

Default demand_scale=**0.2** (smaller than the earlier 0.5 plan).

**Heuristic seed (from prior exps, e.g. Exp 33 with depots=[]):** ~**20 trips / vehicle-day**
supports ~97% served when charging is off. Here charging is on, so treat
`trips_per_day / 20` as a **lower bound** on fleet — the search still brackets
upward if needed.

Efficient search (monotone in fleet and plugs):

1. Exponential bracket on fleet @ plugs=4 (start ≈ ceil(trips/day ÷ 20) unless --start-fleet).
2. Binary search min fleet between fail and pass @ plugs=4.
3. Binary search min plugs/site in [1, p_max] @ that fleet.
4. Binary search min fleet again @ min plugs (unless --no-refine).

Run: PYTHONHASHSEED=0 python3 scripts/run_exp67_fleet_plug_search.py

Options: --demand-scale 0.2 --threshold 95 --start-fleet 9000 --p-max 16 --fleet-cap 50000
"""
from __future__ import annotations

import argparse
import math
import sys
from pathlib import Path

SCRIPT_DIR = Path(__file__).resolve().parent
ROOT = SCRIPT_DIR.parent
sys.path.insert(0, str(ROOT / "backend"))
sys.path.insert(0, str(SCRIPT_DIR))

import run_exp63_continuous_multiday_steady_state as e63  # noqa: E402

# Full synthetic-day row count (see README / RESULTS); load_requests uses round(n * scale).
EST_REQUESTS_FULL_DAY = 867_791

N_SITES = 77
NUM_DAYS = 3
DEFAULT_PLUGS = 4


def heuristic_start_fleet(demand_scale: float, trips_per_veh_day: float = 20.0) -> int:
    """Rough fleet from ~20 trips/veh-day on **calendar** demand (one slice per day)."""
    trips_per_day = max(1, round(EST_REQUESTS_FULL_DAY * demand_scale))
    return max(500, int(math.ceil(trips_per_day / trips_per_veh_day)))


def run_once(
    fleet: int,
    plugs: int,
    demand_scale: float,
    desc: str,
) -> tuple[float, dict]:
    out = e63.run_continuous_experiment(
        N_SITES,
        NUM_DAYS,
        demand_scale=demand_scale,
        fleet_size=fleet,
        plugs_per_site=plugs,
        show_trip_progress=True,
        trip_bar_desc=desc,
    )
    sp = float(out["metrics"]["served_pct"])
    return sp, out


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--demand-scale", type=float, default=0.2, dest="demand_scale")
    ap.add_argument("--threshold", type=float, default=95.0, help="require served_pct > this")
    ap.add_argument("--p-max", type=int, default=16, dest="p_max")
    ap.add_argument("--fleet-cap", type=int, default=50_000)
    ap.add_argument("--no-refine", action="store_true")
    ap.add_argument(
        "--start-fleet",
        type=int,
        default=None,
        help="first bracket probe; default = ceil(trips/day ÷ 20) from demand_scale",
    )
    ap.add_argument(
        "--trips-per-veh-day",
        type=float,
        default=20.0,
        dest="tpd",
        help="only used to compute default --start-fleet",
    )
    args = ap.parse_args()
    scale = float(args.demand_scale)
    thr = args.threshold
    p_max = max(1, args.p_max)
    fleet_cap = args.fleet_cap

    start = (
        args.start_fleet
        if args.start_fleet is not None
        else heuristic_start_fleet(scale, args.tpd)
    )
    tpd_est = round(EST_REQUESTS_FULL_DAY * scale)

    def ok(sp: float) -> bool:
        return sp > thr

    print(
        f"Exp67: demand_scale={scale}, N={N_SITES}, {NUM_DAYS}d continuous, "
        f"target served% > {thr}"
    )
    print(
        f"Heuristic: ~{tpd_est:,} trips/day @ scale → start fleet ≈ {tpd_est}/{args.tpd:.0f} "
        f"= {start:,} (charging may require more than no-depot sweeps)\n"
    )
    print(
        "Search: bracket fleet @ plugs=4 → binary min fleet → binary min plugs → refine fleet\n"
    )

    # --- Phase 1: bracket fleet at plugs=4 ---
    f = max(500, start)
    last_fail: int | None = None
    first_pass: int | None = None
    while f <= fleet_cap:
        sp, _ = run_once(f, DEFAULT_PLUGS, scale, f"exp67_bracket_f{f}_p{DEFAULT_PLUGS}_s{scale}")
        total_plugs = N_SITES * DEFAULT_PLUGS
        print(
            f"  [bracket] fleet={f:,} plugs/site={DEFAULT_PLUGS} total_plugs={total_plugs:,} "
            f"→ served={sp:.3f}%"
        )
        if ok(sp):
            first_pass = f
            break
        last_fail = f
        if f >= fleet_cap:
            break
        f = min(f * 2, fleet_cap) if (last_fail is not None and last_fail < 16_000) else min(
            int(f * 1.5), fleet_cap
        )

    if first_pass is None:
        print(f"\nNo pass up to fleet_cap={fleet_cap:,}. Raise --fleet-cap or --p-max and re-run.")
        sys.exit(2)

    lo = (last_fail + 1) if last_fail is not None else start
    hi = first_pass

    # --- Phase 2: min fleet at plugs=4 ---
    best_f = hi
    while lo < hi:
        mid = (lo + hi) // 2
        sp, _ = run_once(mid, DEFAULT_PLUGS, scale, f"exp67_bsearchF_f{mid}_p{DEFAULT_PLUGS}")
        print(f"  [fleet @ p={DEFAULT_PLUGS}] f={mid:,} → served={sp:.3f}%")
        if ok(sp):
            best_f = mid
            hi = mid
        else:
            lo = mid + 1

    f_star = best_f
    sp_f, out_f = run_once(
        f_star, DEFAULT_PLUGS, scale, f"exp67_confirmF_f{f_star}_p{DEFAULT_PLUGS}"
    )
    print(f"\n** Min fleet @ {DEFAULT_PLUGS} plugs/site: fleet={f_star:,} served={sp_f:.3f}% **")

    # --- Phase 3: min plugs at f_star ---
    sp_p1, _ = run_once(f_star, 1, scale, f"exp67_pluglo_f{f_star}_p1")
    print(f"  [plugs] f={f_star:,} p=1 → served={sp_p1:.3f}%")
    if ok(sp_p1):
        p_star = 1
        sp_p = sp_p1
    else:
        sp_hi, _ = run_once(f_star, p_max, scale, f"exp67_plughi_f{f_star}_p{p_max}")
        print(f"  [plugs] f={f_star:,} p={p_max} → served={sp_hi:.3f}%")
        if not ok(sp_hi):
            print(f"\nEven p={p_max} fails at fleet={f_star:,}. Increase fleet or p-max.")
            sys.exit(3)
        lo_p, hi_p = 1, p_max
        best_p = p_max
        while lo_p < hi_p:
            mid_p = (lo_p + hi_p) // 2
            spm, _ = run_once(f_star, mid_p, scale, f"exp67_bsearchP_f{f_star}_p{mid_p}")
            print(f"  [plugs @ f={f_star:,}] p={mid_p} → served={spm:.3f}%")
            if ok(spm):
                best_p = mid_p
                hi_p = mid_p
            else:
                lo_p = mid_p + 1
        p_star = best_p
        sp_p, _ = run_once(f_star, p_star, scale, f"exp67_confirmP_f{f_star}_p{p_star}")
    print(
        f"\n** Min plugs/site @ fleet={f_star:,}: p={p_star} "
        f"(total chargers={N_SITES * p_star:,}) served={sp_p:.3f}% **"
    )

    # --- Phase 4: minimum fleet at p_star ---
    f_final = f_star
    if not args.no_refine:
        lo2, hi2 = 500, f_star
        best_f2 = f_star
        while lo2 < hi2:
            mid2 = (lo2 + hi2) // 2
            sm, _ = run_once(mid2, p_star, scale, f"exp67_refineF_f{mid2}_p{p_star}")
            print(f"  [refine fleet @ p={p_star}] f={mid2:,} → served={sm:.3f}%")
            if ok(sm):
                best_f2 = mid2
                hi2 = mid2
            else:
                lo2 = mid2 + 1
        f_final = best_f2

    sp_fin, out_fin = run_once(f_final, p_star, scale, f"exp67_final_f{f_final}_p{p_star}")

    m = out_fin["metrics"]
    daily = out_fin["daily"]
    trips = sum(x["arrivals"] for x in daily)
    trips_per_day = trips / max(1, NUM_DAYS)
    eff_tpd = trips / max(1, f_final) / max(1, NUM_DAYS)
    print("\n" + "=" * 90)
    print("FINAL (Exp67)")
    print(
        f"  demand_scale={scale}  N_sites={N_SITES}  days={NUM_DAYS}  "
        f"fleet={f_final:,}  plugs/site={p_star}  total_chargers={N_SITES * p_star:,}"
    )
    print(
        f"  served_pct={m['served_pct']:.3f}  chg_util={m['charger_utilization_pct']:.1f}%  "
        f"fleet_battery_pct={m['fleet_battery_pct']:.2f}  trips={trips:,}"
    )
    print(f"  implied trips/veh-day (this run): {eff_tpd:.2f}  (mean arrivals/day ≈ {trips_per_day:,.0f})")
    print(f"  daily served%: {[round(x['served_pct'], 2) for x in daily]}")


if __name__ == "__main__":
    main()
