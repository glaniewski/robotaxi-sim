# Robotaxi-Sim: Growth & Experimentation Alignment

## Purpose

This project is a **growth + operations co-design simulator** for a Robotaxi platform.

It models how product-level demand steering mechanisms (flexibility prompts, pooling incentives, prebooking nudges, off-peak credits) interact with physical fleet constraints (dispatch, charging queues, repositioning, SOC limits).

The simulator uses:

- Discrete-event fleet modeling
- Real Austin ride-hailing demand (RideAustin dataset)
- H3 spatial indexing (resolution 8)
- OSRM-based travel times with caching
- Configurable repositioning policies (reactive + forecast blend)
- Baseline vs variant A/B comparison via `/compare`

The goal is to quantify how growth levers change:

- P90 wait time
- Served %
- Fleet utilization
- Deadhead %
- Charger congestion
- Cost per trip
- Contribution margin

This connects product experimentation directly to fleet economics.

---

# Mapping to Robotaxi Growth Role

## 1) Develop and implement technical solutions to enhance acquisition, onboarding, retention, and revenue growth

The simulator demonstrates how:

- Reducing P90 wait time improves booking conversion.
- Increasing reliability improves retention.
- Pooling improves utilization and lowers marginal cost.
- Off-peak incentives smooth peak demand and reduce capital requirements.
- Repositioning reduces ETA tail latency without increasing fleet size.

Rather than optimizing growth in isolation, this system models the coupling between:

Product lever → Operational constraint → Economic outcome.

---

## 2) Design and execute A/B tests and experiments to optimize booking conversion and subscription models

Each simulator parameter maps directly to a production experiment:

| Product Lever | Simulator Parameter | Metric Impact |
|---------------|---------------------|---------------|
| Flex departure window | `flex_pct`, `flex_minutes` | Wait time, served % |
| Pooling discount | `pool_pct`, `max_detour_pct` | Utilization, deadhead |
| Prebooking prompt | `prebook_pct`, `eta_threshold_minutes` | Peak smoothing |
| Off-peak credit | `offpeak_shift_pct` | Charger queue reduction |
| Repositioning policy | `reposition_enabled`, `reposition_alpha` | P90 ETA tail |

The `/compare` endpoint produces deltas:

- Δ P90 wait
- Δ served %
- Δ utilization
- Δ deadhead %
- Δ charger queue P90
- Δ contribution margin

This mirrors real A/B experimentation frameworks.

---

## 3) Collaborate cross-functionally and translate growth opportunities into technical requirements

The simulator creates a shared evaluation environment:

- Product: What behavioral nudge changes booking patterns?
- Operations: Does that overload charging?
- Finance: Does it improve margin?
- Marketing: What incentive level is economically sustainable?

Example:
A 10% flex-window adoption rate may reduce required fleet size by X%, lowering capital intensity.

This turns growth ideas into quantitative operational tradeoffs.

---

## 4) Analyze user behavior and service metrics to uncover bottlenecks

Using real RideAustin-derived demand, the simulator surfaces:

- Spatial ETA hotspots
- Peak congestion windows
- Charger saturation periods
- Deadhead inefficiency patterns
- Service inequities by H3 cell

These bottlenecks directly impact:

- Conversion rate
- Cancellation rate
- Retention

---

## 5) Build tools to automate and optimize growth processes

The simulator enables:

- Automated scenario comparison
- Fleet sizing sensitivity curves
- SLA feasibility frontiers (fleet size vs P90 wait)
- Repositioning policy tuning (reactive vs proactive blend)

It functions as an internal experimentation engine for mobility growth strategy.

---

# Strategic Insight

In an autonomous fleet, marginal cost per trip can approach energy + maintenance.

At that point, demand steering becomes a capital allocation tool.

Shifting 5–10% of peak demand through product nudges can:

- Reduce required fleet size
- Reduce charger buildout
- Increase utilization
- Improve revenue predictability

Demand is not exogenous.

It is shapeable.

This simulator quantifies that control surface.