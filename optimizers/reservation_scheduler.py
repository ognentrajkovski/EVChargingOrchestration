"""
Reservation-Based EV Charging Scheduler
=========================================
Pure planning functions — no Flink/Kafka imports.

Cars reserve the cheapest future time slots based on projected prices.
Subsequent cars see updated projected prices reflecting existing reservations,
enabling fleet-wide coordination to spread demand across cheaper slots.
"""

from math import ceil

# ---------------------------------------------------------------------------
# Simulation Constants (shared with heuristic_scheduler.py)
# ---------------------------------------------------------------------------
BASE_PRICE = 50.0
STATION_CAPACITY = 5

# State-based price multipliers — must match produce_energy_data.py PRICE_LEVELS
# (occupancy_ratio_upper_bound, multiplier)
_PRICE_LEVELS = [
    (0.30, 0.6),   # off-peak
    (0.60, 1.0),   # normal
    (1.01, 1.8),   # peak
]
MAX_POWER_PER_CHARGER = 11.0   # kW per charger
CAR_BATTERY_CAPACITY_KWH = 60.0
SLOTS_PER_DAY = 96
ENERGY_PER_SLOT = MAX_POWER_PER_CHARGER * 0.25  # 2.75 kWh per 15-min slot

# Replan triggers
SOC_DROP_THRESHOLD = 0.10      # replan if SOC dropped 10%+ since last plan
PERIODIC_REPLAN_SLOTS = 16     # replan every 16 slots (~4 hours)


def target_soc(current_soc: float) -> float:
    """Target SOC by current SOC band.

    < 15% → emergency, get to 50%
    15-50% → needs charge, fill to 100%
    > 50% → don't plan (return -1 so slots_needed() yields 0)
    """
    if current_soc < 0.15:
        return 0.50   # emergency → get to 50%
    if current_soc < 0.50:
        return 1.00   # needs charge → fill up fully
    return -1.0       # above 50% → don't plan


def slots_needed(current_soc: float) -> int:
    """How many 15-min slots are needed to reach target SOC.

    For non-emergency cars, the cheapest slots are spread across the day rather
    than being consecutive.  The car continues discharging between those slots,
    so we add a 15 % buffer on the energy delta to compensate for the expected
    discharge between charging events.  Emergency cars charge consecutively so
    no buffer is applied for them.
    """
    target = target_soc(current_soc)
    energy_needed = (target - current_soc) * CAR_BATTERY_CAPACITY_KWH
    if energy_needed <= 0:
        return 0
    is_emergency = current_soc < 0.15
    if not is_emergency:
        energy_needed *= 1.15  # 15 % buffer for discharge between spread slots
    return ceil(energy_needed / ENERGY_PER_SLOT)


def projected_price(reservation_count: int) -> float:
    """Price for a slot given how many reservations already exist.
    Uses the same state-based multipliers as the energy producer."""
    ratio = reservation_count / max(STATION_CAPACITY, 1)
    for upper, mult in _PRICE_LEVELS:
        if ratio <= upper:
            return BASE_PRICE * mult
    return BASE_PRICE * _PRICE_LEVELS[-1][1]


def should_replan(car_reservation: dict, current_idx: int, current_soc: float) -> bool:
    """
    Decide whether a car needs to replan its reservations.

    Parameters
    ----------
    car_reservation : dict or None — {"reserved_slots": [...], "planned_soc": float, "planned_at_idx": int}
    current_idx     : current simulation slot index
    current_soc     : car's current state of charge
    """
    if car_reservation is None:
        return True

    reserved_slots = car_reservation.get('reserved_slots', [])
    planned_soc = car_reservation.get('planned_soc', 1.0)
    planned_at_idx = car_reservation.get('planned_at_idx', 0)

    # No reservations exist
    if not reserved_slots:
        return True

    # SOC dropped 10%+ since last plan
    if planned_soc - current_soc >= SOC_DROP_THRESHOLD:
        return True

    # Periodic replan every 16 slots
    if current_idx - planned_at_idx >= PERIODIC_REPLAN_SLOTS:
        return True

    # All reserved slots are in the past
    if all(s < current_idx for s in reserved_slots):
        return True

    return False


def select_cheapest_slots(
    current_idx: int,
    num_needed: int,
    reservation_counts: dict,
    is_emergency: bool = False,
) -> list:
    """
    Pick the cheapest future slots for charging.

    Parameters
    ----------
    current_idx        : current slot index (0–95)
    num_needed         : number of slots to reserve
    reservation_counts : {slot_idx: count} — existing reservations per slot
    is_emergency       : if True, pick consecutive slots starting now

    Returns
    -------
    list[int] : sorted list of selected slot indices
    """
    if num_needed <= 0:
        return []

    if is_emergency:
        # Pick num_needed slots starting at current_idx, skipping full ones
        selected = []
        for slot in range(current_idx, SLOTS_PER_DAY):
            if len(selected) >= num_needed:
                break
            count = reservation_counts.get(slot, 0)
            if count >= STATION_CAPACITY:
                continue
            selected.append(slot)
        return selected

    # Build (projected_price, slot_idx) for future slots, skip full slots
    candidates = []
    for slot in range(current_idx, SLOTS_PER_DAY):
        count = reservation_counts.get(slot, 0)
        if count >= STATION_CAPACITY:
            continue  # slot is full, no charger available
        price = projected_price(count)
        candidates.append((price, slot))

    # Sort by price ascending, then by slot index for stability
    candidates.sort(key=lambda x: (x[0], x[1]))

    # Pick the cheapest num_needed slots
    selected = [slot for _, slot in candidates[:num_needed]]
    selected.sort()  # return in chronological order
    return selected
