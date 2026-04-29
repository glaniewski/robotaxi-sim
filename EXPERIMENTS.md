# Public Experiment Summary

This file is the curated version of the simulation results behind the robotaxi
blog post. It is distilled from the raw development logs and run artifacts,
which are intentionally noisier because they include failed runs, bug fixes, and
intermediate hypotheses.

The results below focus on the final experiment families used in the public
writeup: fleet sizing, depot geography, charger configuration, battery sizing,
and congestion sensitivity.

## Methodology Snapshot

- Demand: 867,791 cleaned RideAustin trips collapsed into a representative
  Austin 24-hour demand profile.
- Geography: Austin trip footprint indexed with H3 resolution 8 cells.
- Routing: OSRM travel times with an H3-to-H3 routing cache.
- Simulation: deterministic discrete-event model with seeded randomness.
- Vehicle states: pickup, in-trip, repositioning, depot travel, charging, and
  idle.
- Steady-state approach: many public-facing charts use a three-day continuous
  simulation clock and report day-three behavior so vehicles have completed
  multiple charge cycles.

The numbers are scenario estimates, not forecasts. They are most useful for
comparing tradeoffs under controlled assumptions.

## 1. Fleet Size Has A Knee

Question: at what fleet size does a 95% served target become achievable, and
what happens to economics past that point?

Setup: 77 distributed charging sites, 10 plugs per site, 11.5 kW per plug,
0.20 kWh/mi vehicles, three-day continuous simulation.

| Fleet size | Served demand | Trips / vehicle / day | Interpretation |
|---:|---:|---:|---|
| 1,500 | 63% | High utilization | Fleet-limited |
| 3,000 | 90% | ~52 | Near the efficient frontier |
| 4,000 | ~95% | ~40 | Service target becomes achievable |
| 4,500 | ~96% | ~37 | Strong service, lower marginal returns |
| 5,500 | ~97% | ~31 | Extra cars mostly buy buffer |

Finding: the service curve has a sharp knee around 4,000 vehicles. Adding cars
below the knee directly recovers unserved trips. Adding cars above it mostly
reduces tail wait time and coverage gaps while depreciation cost per completed
trip rises.

## 2. Charger Placement Beats Charger Count

Question: if total installed charging capacity is held roughly constant, does
depot geography matter?

Setup: 4,500 vehicles, same demand, same three-day clock, roughly equal total
installed charging power.

| Configuration | Served demand | Charger utilization | Deadhead | p90 wait |
|---|---:|---:|---:|---:|
| 2 mega-depots | 93.9% | 64.2% | 33.1% | 7.5 min |
| 5 large depots | 81.7% | 54.7% | 28.7% | 7.0 min |
| 20 medium depots | 93.6% | 65.2% | 32.3% | 7.2 min |
| 77 microsites | 95.1% | 71.8% | 36.8% | 7.0 min |

Finding: more sites is not automatically better. The two successful strategies
were either to concentrate charging exactly where demand is densest, or to cover
the full operating footprint with small sites. The intermediate five-depot
configuration performed worst because it did neither.

## 3. Plug Availability Beats Charger Speed

Question: at fixed total installed power, is it better to buy fewer fast
chargers or more slower plugs?

Setup: 77 depots, roughly 8.9 MW total installed power, same fleet, battery, and
demand. Only the split between plug count and kW per plug changes.

| Plugs / site | kW / plug | Served demand | Charger utilization | Fleet SOC |
|---:|---:|---:|---:|---:|
| 1 | 115.0 | 92.3% | 80.2% | 65.9% |
| 2 | 57.5 | 95.3% | 92.1% | 66.8% |
| 4 | 28.8 | 96.1% | 98.0% | 63.5% |
| 7 | 16.5 | 96.1% | 99.6% | 63.8% |
| 10 | 11.5 | 95.8% | 100.3% | 63.6% |
| 20 | 5.8 | 95.2% | 101.6% | 64.8% |

Finding: the peak is 4-7 plugs per site. A single 115 kW plug per site performs
worse than several slower plugs because vehicles lose time to slot contention
and failed depot attempts. Once contention disappears, additional plugs provide
little benefit.

Note: utilization above 100% is a measurement artifact from counting planned
charging sessions that begin near the simulation horizon and extend beyond it.
Served demand is unaffected.

## 4. Battery Floor Depends On Depot Density

Question: how small can the vehicle battery get before service quality breaks?

Setup: 4,500 vehicles, 10 plugs per site, 11.5 kW per plug, battery capacity
swept from 75 kWh down to 15 kWh under two depot geographies.

| Battery size | Distributed network: 77 depots | Centralized network: 2 depots |
|---:|---:|---:|
| 75 kWh | ~95.8% served | ~93.9% served |
| 40 kWh | ~95.2% served | ~93.9% served |
| 30 kWh | ~93.6% served | ~93.9% served |
| 15 kWh | ~89.5% served | ~93.9% served |

Finding: in the distributed network, 40 kWh is a practical floor with less than
one point of service impact relative to 75 kWh. Below 30 kWh, charging
interruptions start to compete with dispatch. In the centralized network,
battery size appears irrelevant because depot travel time, not pack size, is
already the binding constraint.

## 5. Congestion Is The Biggest Missing Realism Layer

Question: how sensitive are the findings to slower city travel times?

Setup: fixed fleet and infrastructure, OSRM travel times multiplied by a uniform
congestion factor.

Finding: every 50% increase in ambient travel time costs roughly 10 percentage
points of served demand at fixed fleet size. The mechanism is simple: longer
trips and deadhead legs reduce effective vehicle capacity. This is the largest
known optimism in the model and the first realism layer I would add next.

## What The Results Mean

The most important conclusion is that robotaxi operations are an infrastructure
optimization problem as much as a dispatch problem. In this model:

- Fleet sizing has a clear knee; above it, extra vehicles buy reliability more
  than volume.
- Charger geography matters more than raw charger count.
- Plug concurrency can matter more than charger speed at fixed power.
- Battery capacity only matters after depot geometry is good enough for battery
  limits to become visible.
- Congestion can dominate all of the above by reducing fleet throughput.

The simulator is intentionally transparent: assumptions, routing, dispatch,
charging policy, and experiment artifacts are all in this repository so the
results can be challenged or extended.
