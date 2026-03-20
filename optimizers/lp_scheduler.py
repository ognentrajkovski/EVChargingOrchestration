import time
import numpy as np
from ortools.linear_solver import pywraplp

# ── Simulation Constants (identical to heuristic_scheduler.py) ────────────────
SIMULATION_INTERVAL_MINS  = 60
TOTAL_INTERVALS           = 24
STATION_CAPACITY          = 5
MAX_POWER_PER_CHARGER     = 11.0   # kW
CAR_BATTERY_CAPACITY_KWH  = 60.0
ENERGY_PER_INTERVAL_KWH   = MAX_POWER_PER_CHARGER * (SIMULATION_INTERVAL_MINS / 60.0)  # 2.75 kWh

# ── Thresholds ─────────────────────────────────────────────────────────────────
MIN_SOC_EMERGENCY  = 0.15
MAX_SESSIONS_PER_DAY = 3

# ── Target SOC by current SOC band (mirrors heuristic strategy tiers) ─────────
# The LP will try to reach these targets at minimum cost.
def _target_soc(current_soc: float) -> float:
    if current_soc < 0.15: return 0.40   # Emergency  → get to 40%
    if current_soc < 0.40: return 0.60   # Low        → get to 60%
    if current_soc < 0.60: return 0.80   # Medium     → get to 80%
    if current_soc < 0.80: return 1.00   # High       → get to 100%
    return 1.00                           # Nearly full → top up if cheap


def smart_lp_schedule(prices, cars, current_idx=0, sessions_used=None, station_capacity=None):
    """
    Drop-in replacement for smart_heuristic_schedule().

    Uses Google OR-Tools Mixed-Integer Linear Programming to find the
    provably optimal (minimum cost) charging schedule for the fleet,
    subject to:
        - Battery capacity constraints (SOC between 0 and 100%)
        - Station capacity constraint  (max N cars charging simultaneously)
        - Session limit                (max MAX_SESSIONS_PER_DAY per car)
        - Emergency safety constraint  (cars below 15% SOC must charge immediately)

    Returns the same dict format as the heuristic:
        { car_id: [ {"interval": t, "power_kw": kw, "price": p}, ... ] }
    """

    # ── Input validation (identical guards to heuristic) ──────────────────────
    if not prices:
        return {c['id']: [] for c in cars if c.get('id')}

    prices = [
        max(0.0, float(p)) if (p is not None and not np.isnan(float(p))) else 999.0
        for p in prices
    ]
    cars = [
        c for c in cars
        if c.get('id') and c.get('current_soc') is not None and c.get('current_battery_kwh') is not None
    ]
    if not cars:
        return {}

    total_intervals = len(prices)
    horizon         = range(current_idx, total_intervals)   # slots we can still plan
    _capacity       = station_capacity if (station_capacity and station_capacity > 0) else STATION_CAPACITY
    _used           = sessions_used or {}

    start_time = time.time()

    # ── Create solver ──────────────────────────────────────────────────────────
    # CBC = open-source mixed-integer solver bundled with OR-Tools
    solver = pywraplp.Solver.CreateSolver('CBC')
    if not solver:
        # Fallback: if OR-Tools not available, raise clearly
        raise RuntimeError("OR-Tools CBC solver could not be created. Run: pip install ortools")

    solver.SetTimeLimit(1000)   # 1 000 ms hard cap — never blocks Flink tick

    # ── Decision variables ─────────────────────────────────────────────────────
    # x[car_id, t] ∈ {0, 1}  →  1 = charge car at interval t, 0 = don't
    x = {}
    for car in cars:
        cid = car['id']
        for t in horizon:
            x[cid, t] = solver.BoolVar(f'x_{cid}_{t}')

    # session_start[car_id, t] ∈ {0,1} → 1 = a NEW charging session starts at t
    # Used to count sessions without penalising cost (pure feasibility variable).
    session_start = {}
    for car in cars:
        cid = car['id']
        for t in horizon:
            session_start[cid, t] = solver.BoolVar(f's_{cid}_{t}')

    # ── Objective: minimise total electricity cost ─────────────────────────────
    # cost = Σ price[t] × energy_per_interval × x[car, t]   for all cars, all t
    # (ENERGY_PER_INTERVAL_KWH / 1000 converts kWh × €/MWh → €)
    solver.Minimize(solver.Sum(
        prices[t] * (ENERGY_PER_INTERVAL_KWH / 1000.0) * x[cid, t]
        for car in cars
        for cid in [car['id']]
        for t in horizon
    ))

    # ── Constraints ────────────────────────────────────────────────────────────
    for car in cars:
        cid         = car['id']
        current_kwh = float(car['current_battery_kwh'])
        current_soc = float(car['current_soc'])
        target      = _target_soc(current_soc)
        target_kwh  = target * CAR_BATTERY_CAPACITY_KWH
        sessions_already_used = _used.get(cid, 0)

        # ── 1. Must reach target SOC by end of horizon ─────────────────────
        # Σ x[cid, t] × energy_per_interval  ≥  energy_needed
        energy_needed = max(0.0, target_kwh - current_kwh)
        solver.Add(
            solver.Sum(x[cid, t] * ENERGY_PER_INTERVAL_KWH for t in horizon)
            >= energy_needed
        )

        # ── 2. Battery cannot exceed capacity at any point ─────────────────
        # current_kwh + Σ_{τ ≤ t} energy × x[cid,τ]  ≤  CAR_BATTERY_CAPACITY_KWH
        for t in horizon:
            solver.Add(
                current_kwh + solver.Sum(
                    x[cid, tau] * ENERGY_PER_INTERVAL_KWH
                    for tau in range(current_idx, t + 1)
                ) <= CAR_BATTERY_CAPACITY_KWH
            )

        # ── 3. Emergency constraint — must charge immediately ──────────────
        # If SOC < 15%, force x[cid, current_idx] = 1
        if current_soc < MIN_SOC_EMERGENCY and current_idx in range(total_intervals):
            solver.Add(x[cid, current_idx] == 1)

        # ── 4. Session counting ────────────────────────────────────────────
        # A session starts at t if: x[t]=1 AND x[t-1]=0  (or t is the first slot)
        # session_start[t] ≥ x[t] - x[t-1]
        # Σ session_start[t]  ≤  MAX_SESSIONS_PER_DAY - sessions_already_used
        sessions_remaining = max(0, MAX_SESSIONS_PER_DAY - sessions_already_used)
        slots = list(horizon)
        for i, t in enumerate(slots):
            if i == 0:
                # First available slot: session starts if charging at all
                solver.Add(session_start[cid, t] >= x[cid, t])
            else:
                prev_t = slots[i - 1]
                if t == prev_t + 1:
                    # Contiguous slot: session starts only on 0→1 transition
                    solver.Add(session_start[cid, t] >= x[cid, t] - x[cid, prev_t])
                else:
                    # Gap between slots (shouldn't happen but guard anyway)
                    solver.Add(session_start[cid, t] >= x[cid, t])
            # session_start is an upper-bounded indicator
            solver.Add(session_start[cid, t] <= x[cid, t])

        solver.Add(
            solver.Sum(session_start[cid, t] for t in horizon) <= sessions_remaining
        )

    # ── 5. Station capacity constraint ────────────────────────────────────────
    # At every interval t: Σ_cars x[car, t]  ≤  _capacity
    for t in horizon:
        solver.Add(
            solver.Sum(x[car['id'], t] for car in cars) <= _capacity
        )

    # ── Solve ──────────────────────────────────────────────────────────────────
    status = solver.Solve()

    elapsed = time.time() - start_time
    # print(f"LP solved in {elapsed:.4f}s | status={status} | vars={solver.NumVariables()} | constraints={solver.NumConstraints()}")

    # ── Extract solution ───────────────────────────────────────────────────────
    schedule = {car['id']: [] for car in cars}

    if status in (pywraplp.Solver.OPTIMAL, pywraplp.Solver.FEASIBLE):
        for car in cars:
            cid = car['id']
            for t in horizon:
                if x[cid, t].solution_value() > 0.5:   # binary — 0.5 threshold for float safety
                    schedule[cid].append({
                        "interval":  t,
                        "power_kw":  MAX_POWER_PER_CHARGER,
                        "price":     prices[t],
                    })
    else:
        # Solver failed (infeasible or timeout) — fall back to emergency-only charging
        # so cars below 15% SOC still get charged rather than being left stranded.
        # print(f"LP solver returned status {status} — applying emergency fallback")
        for car in cars:
            cid = car['id']
            if float(car['current_soc']) < MIN_SOC_EMERGENCY:
                for t in list(horizon)[:4]:   # charge next 4 slots (1 hour) as safety net
                    schedule[cid].append({
                        "interval":  t,
                        "power_kw":  MAX_POWER_PER_CHARGER,
                        "price":     prices[t],
                    })

    return schedule
