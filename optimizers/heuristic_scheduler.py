"""
Bidding-based EV Charging Decision Module
==========================================
Replaces the 48-hour day-ahead price planner with a real-time, per-tick
decision function. The charging cost is driven entirely by how many chargers
are currently occupied (see produce_energy_data.py for the price formula).

Each car decides on every Flink tick whether to start or stop charging based
on:
  1. Battery level (SOC) — below EMERGENCY_SOC_THRESHOLD, always charge.
  2. Acceptable price band — per-SOC multiplier applied to BASE_PRICE.
"""

# ---------------------------------------------------------------------------
# Simulation Constants
# ---------------------------------------------------------------------------
STATION_CAPACITY = 5          # Total number of physical chargers

# Baseline price (EUR/MWh) — matches producer's BASE_PRICE constant.
# This is what you pay when zero chargers are occupied.
BASE_PRICE = 50.0

# How aggressively price rises with occupancy (see price formula in producer).
#   price = BASE_PRICE * (1 + PRICE_COEFFICIENT * active / total)
# At full occupancy: price = BASE_PRICE * (1 + 1.5) = 2.5 × base
PRICE_COEFFICIENT = 1.5

MAX_POWER_PER_CHARGER = 11.0   # kW per charger
CAR_BATTERY_CAPACITY_KWH = 60.0

# ---------------------------------------------------------------------------
# SOC Thresholds & Acceptable Price Bands
# ---------------------------------------------------------------------------
# Emergency: car always charges regardless of price
EMERGENCY_SOC_THRESHOLD = 0.15

# For each SOC band the car will charge only if current_price <=
# BASE_PRICE * ACCEPTABLE_MULTIPLIER[band].
# Lower SOC → higher multiplier → more willing to pay.
_SOC_BANDS = [
    # (soc_upper_bound, acceptable_price_multiplier)
    (0.30, 2.00),   # 15–30%  very low  — will pay up to 2.0× base (3 chargers busy)
    (0.50, 1.60),   # 30–50%  low       — will pay up to 1.6× base (2 chargers busy)
    (0.80, 1.20),   # 50–80%  medium    — will pay up to 1.2× base (0 chargers busy, requires empty station to start)
    (1.01, 1.05),   # ≥ 80%   high      — opportunistic, up to 1.05× base (0 chargers busy)
]


def acceptable_price(soc: float, is_charging: bool = False) -> float:
    """
    Return the maximum price (EUR/MWh) this car is willing to pay at the
    given state-of-charge.  Below EMERGENCY_SOC_THRESHOLD the car will charge
    regardless, so this value is only consulted for non-emergency decisions.
    
    Hysteresis is applied if the car is already charging to prevent rapid 
    plug/unplug ping-ponging when its own charging triggers a price spike.
    """
    base_multiplier = 0.50
    for upper, multiplier in _SOC_BANDS:
        if soc < upper:
            base_multiplier = multiplier
            break
            
    # Hysteresis bonus: if we already hold a slot, we are willing to pay 
    # +0.6x base price more to keep it, so we don't bounce out instantly
    if is_charging:
        base_multiplier += 0.60
        
    return BASE_PRICE * base_multiplier


def decide_charge(car: dict, current_price: float, is_charging: bool = False) -> bool:
    """
    Real-time bidding decision for a single car.

    Parameters
    ----------
    car           : car state dict, must contain 'current_soc' (0.0 – 1.0)
    current_price : current dynamic charging price in EUR/MWh
    is_charging   : whether the car is already actively charging this tick

    Returns
    -------
    True  → car should start / continue charging this tick
    False → car should stop / remain idle this tick
    """
    soc = car.get('current_soc', 1.0)

    # --- Full Battery Override ---
    if soc >= 0.99:
        return False  # Fully charged, must yield charger

    # --- Safety override ---
    if soc < EMERGENCY_SOC_THRESHOLD:
        return True  # charge at any price

    # --- Price-sensitive decision ---
    threshold = acceptable_price(soc, is_charging)
    return current_price <= threshold