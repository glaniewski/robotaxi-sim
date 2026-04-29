"""
FIFO depot charging queue vs JIT replan (charging_queue_policy).
"""
from __future__ import annotations

import pytest

from app.sim.engine import SimConfig, SimulationEngine, build_vehicles
from app.sim.entities import Depot, VehicleState
from app.sim.routing import RoutingCache

from .conftest import AUSTIN_CELLS, _build_mock_cache, make_requests


def test_fifo_records_queue_wait_when_plugs_collide(monkeypatch: pytest.MonkeyPatch) -> None:
    """
    Force reservation slotting to ignore existing jobs so two vehicles target the same
    arrival window; first occupies the plug, second waits in depot.queue under fifo.

    With hard-lock reservations the monkeypatch makes both vehicles believe they have
    a guaranteed slot.  To still exercise the FIFO queue path we also strip the
    second vehicle's reservation right before arrival so it arrives without a lock.
    """
    monkeypatch.setattr(
        SimulationEngine,
        "_earliest_slot_start",
        staticmethod(
            lambda arrival_earliest, duration_s, jobs, capacity: float(arrival_earliest)
        ),
    )

    config = SimConfig(
        duration_minutes=120.0,
        seed=1,
        fleet_size=2,
        soc_initial=0.25,
        soc_target=0.80,
        soc_charge_start=0.80,
        reposition_enabled=False,
        timeseries_bucket_minutes=30.0,
        charging_queue_policy="fifo",
    )
    depot = Depot(
        id="depot_0",
        h3_cell=AUSTIN_CELLS[0],
        chargers_count=1,
        charger_kw=150.0,
        site_power_kw=150.0,
    )
    routing = RoutingCache(cache=_build_mock_cache())
    vehicles = build_vehicles(config, [AUSTIN_CELLS[0]], seed=1)
    requests = make_requests(0, seed=1)

    engine = SimulationEngine(
        config=config,
        vehicles=vehicles,
        requests=requests,
        depots=[depot],
        routing=routing,
    )

    _orig_arrive = engine._handle_arrive_depot.__func__  # type: ignore[attr-defined]
    _seen_first = {"done": False}

    def _patched_arrive(self, event):
        if _seen_first["done"]:
            vid = event.payload["vehicle_id"]
            self._clear_charge_reservation(vid)
        else:
            _seen_first["done"] = True
        _orig_arrive(self, event)

    patched = lambda ev: _patched_arrive(engine, ev)
    monkeypatch.setattr(engine, "_handle_arrive_depot", patched)
    from app.sim.events import EventType as _ET
    engine._handlers[_ET.ARRIVE_DEPOT] = patched

    engine.run()

    assert depot.queue == []
    assert engine._depot_queue_waits, "expected non-zero queue wait samples"
    assert engine._depot_queue_max_at_site >= 1
    for v in engine.vehicles.values():
        assert v.soc >= config.soc_target - 1e-6
        assert v.state == VehicleState.IDLE


def test_invalid_charging_queue_policy_raises() -> None:
    config = SimConfig(charging_queue_policy="nope")  # type: ignore[arg-type]
    with pytest.raises(ValueError, match="charging_queue_policy"):
        SimulationEngine(
            config=config,
            vehicles=[],
            requests=[],
            depots=[],
            routing=RoutingCache(cache=_build_mock_cache()),
        )
