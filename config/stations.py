"""
Charging Station Configuration
================================
Single source of truth for all station definitions and shared physical constants.
Import from here instead of defining BASE_PRICE / STATION_CAPACITY inline.
"""

import math

# ---------------------------------------------------------------------------
# Physical constants (shared across all stations)
# ---------------------------------------------------------------------------
MAX_POWER_PER_CHARGER       = 11.0          # kW per charger
ENERGY_PER_SLOT             = 11.0 * 0.25  # 2.75 kWh per simulated 15-min slot
MOVE_STEP                   = 0.1           # km per tick — car moves ±MOVE_STEP per axis (random walk)
TRAVEL_STEP_KM              = 0.2           # km per tick — directed travel toward a station (2× random)
ARRIVE_THRESHOLD_KM         = 0.3           # km — car is considered "at station" within this distance
CONSUMPTION_KWH_PER_KM      = 0.2           # EV energy consumption while driving
ROUND_TRIP_FACTOR           = 2             # multiply one-way distance for round trip

# ---------------------------------------------------------------------------
# Station definitions
# Coordinates are in km on a 20 × 20 grid.
# base_price: EUR/MWh — used for both dynamic pricing and travel-cost calculation.
# capacity  : number of physical chargers at that station.
# ---------------------------------------------------------------------------
STATIONS = {
    "station_A": {"name": "Alpha",    "x":  2.0, "y":  2.0, "base_price": 48.0, "capacity": 6},
    "station_B": {"name": "Beta",     "x":  2.0, "y": 10.0, "base_price": 45.0, "capacity": 5},
    "station_C": {"name": "Gamma",    "x":  2.0, "y": 18.0, "base_price": 48.0, "capacity": 6},
    "station_D": {"name": "Delta",    "x": 10.0, "y":  2.0, "base_price": 55.0, "capacity": 7},
    "station_E": {"name": "Epsilon",  "x": 10.0, "y":  6.0, "base_price": 62.0, "capacity": 7},
    "station_F": {"name": "Zeta",     "x": 10.0, "y": 10.0, "base_price": 72.0, "capacity": 8},
    "station_G": {"name": "Eta",      "x": 10.0, "y": 14.0, "base_price": 62.0, "capacity": 7},
    "station_H": {"name": "Theta",    "x": 10.0, "y": 18.0, "base_price": 55.0, "capacity": 6},
    "station_I": {"name": "Iota",     "x": 18.0, "y":  2.0, "base_price": 45.0, "capacity": 5},
    "station_J": {"name": "Kappa",    "x": 18.0, "y": 10.0, "base_price": 48.0, "capacity": 6},
    "station_K": {"name": "Lambda",   "x": 18.0, "y": 18.0, "base_price": 45.0, "capacity": 5},
    "station_L": {"name": "Mu",       "x":  5.0, "y":  5.0, "base_price": 52.0, "capacity": 6},
    "station_M": {"name": "Nu",       "x":  5.0, "y": 15.0, "base_price": 50.0, "capacity": 5},
    "station_N": {"name": "Xi",       "x": 15.0, "y":  5.0, "base_price": 52.0, "capacity": 6},
    "station_O": {"name": "Omicron",  "x": 15.0, "y": 15.0, "base_price": 50.0, "capacity": 6},
    "station_P": {"name": "Pi",       "x":  7.0, "y": 10.0, "base_price": 58.0, "capacity": 7},
    "station_Q": {"name": "Rho",      "x": 13.0, "y": 10.0, "base_price": 58.0, "capacity": 7},
    "station_R": {"name": "Sigma",    "x": 10.0, "y":  4.0, "base_price": 56.0, "capacity": 6},
    "station_S": {"name": "Tau",      "x": 10.0, "y": 16.0, "base_price": 56.0, "capacity": 6},
    "station_T": {"name": "Upsilon",  "x":  6.0, "y": 18.0, "base_price": 47.0, "capacity": 5},
}

# ---------------------------------------------------------------------------
# Shared price-level multipliers (must match produce_energy_data.py)
# ---------------------------------------------------------------------------
PRICE_LEVELS = [
    (0.30, 0.6),   # off-peak  : ≤ 30 % occupied → 0.6 × base_price
    (0.60, 1.0),   # normal    : ≤ 60 % occupied → 1.0 × base_price
    (1.01, 1.8),   # peak      : > 60 % occupied → 1.8 × base_price
]

DECISION_MODE = 'heuristic'


# ---------------------------------------------------------------------------
# Helper functions
# ---------------------------------------------------------------------------

def station_distance_km(car_x: float, car_y: float, station_id: str) -> float:
    """Euclidean distance in km from a car's current position to a station."""
    s = STATIONS[station_id]
    return math.sqrt((car_x - s["x"]) ** 2 + (car_y - s["y"]) ** 2)


def travel_cost_eur(car_x: float, car_y: float, station_id: str) -> float:
    """
    Round-trip energy cost in EUR for a car to drive to a station and back.

    Formula:
        cost = 2 × distance_km × CONSUMPTION_KWH_PER_KM × station.base_price / 1000
        (base_price is EUR/MWh; divide by 1000 to convert to EUR/kWh)
    """
    dist = station_distance_km(car_x, car_y, station_id)
    base_price = STATIONS[station_id]["base_price"]
    return ROUND_TRIP_FACTOR * dist * CONSUMPTION_KWH_PER_KM * base_price / 1000.0


def projected_price_for_station(reservation_count: int, station_id: str) -> float:
    """State-based projected price for a slot at a given station."""
    stn = STATIONS[station_id]
    ratio = reservation_count / max(stn["capacity"], 1)
    for upper, mult in PRICE_LEVELS:
        if ratio <= upper:
            return stn["base_price"] * mult
    return stn["base_price"] * PRICE_LEVELS[-1][1]
