"""
Q-Learning Agent for EV Charging Station Selection
===================================================
Replaces the NetworkX graph-based station selection (fleet_graph.select_station_and_slots)
with a Q-learning policy that learns which station to pick based on observed state.

State space (7-tuple):
  (soc_band, price_A_level, occ_A_level, price_B_level, occ_B_level, price_C_level, occ_C_level)

  soc_band          : 0=low(<30%), 1=mid(<50%), 2=high(>=50%)
  price_X_level     : 0=off-peak, 1=normal, 2=peak  (relative to station base_price)
  occ_X_level       : 0=free, 1=half-full, 2=full

Total states: 3 × (3×3)^3 = 2187 — stored as a Python dict, ~17 KB.

Actions: ['station_A', 'station_B', 'station_C', None]
  None means "don't book this tick" (used when all stations are too expensive).

The agent is instantiated once per Flink job in open() and lives in memory for
the duration of the run. No persistence across restarts — that is intentional;
the learning curve visible on the dashboard is part of the comparison story.
"""

import random
import math

# Import STATIONS lazily so this module can be syntax-checked without config/ on the path.
# The actual import happens the first time encode_state / compute_reward are called.
_STATIONS = None

def _get_stations():
    global _STATIONS
    if _STATIONS is None:
        from stations import STATIONS
        _STATIONS = STATIONS
    return _STATIONS


# ---------------------------------------------------------------------------
# Hyperparameters
# ---------------------------------------------------------------------------
ALPHA         = 0.15    # learning rate
GAMMA         = 0.85    # discount factor
EPSILON_START = 0.80    # initial exploration probability
EPSILON_MIN   = 0.10    # floor — never stop exploring entirely
EPSILON_DECAY = 0.997   # multiplied after every Q-update

ACTIONS = ['station_A', 'station_B', 'station_C', None]


# ---------------------------------------------------------------------------
class QLearningAgent:
    """
    Epsilon-greedy Q-learning agent for EV charging station selection.

    Public attributes (read by dashboard via RESERVATION_STATUS):
        epsilon        : float — current exploration rate
        steps          : int   — total Q-table updates so far
        reward_history : list  — last 200 per-update rewards
    """

    def __init__(self):
        self.q_table: dict = {}        # state_tuple → list[float]  (one per action)
        self.epsilon: float = EPSILON_START
        self.steps:   int   = 0
        self.reward_history: list = []

    # ── Internal ───────────────────────────────────────────────────────────

    def _q(self, state: tuple) -> list:
        """Return (and lazily initialise) the Q-values for a state."""
        if state not in self.q_table:
            self.q_table[state] = [0.0] * len(ACTIONS)
        return self.q_table[state]

    # ── Public API ─────────────────────────────────────────────────────────

    def encode_state(
        self,
        soc: float,
        station_prices: dict,
        station_res_counts: dict,
    ) -> tuple:
        """
        Encode the observable environment into a discrete state tuple.

        Parameters
        ----------
        soc                : car's state of charge (0.0 – 1.0)
        station_prices     : {station_id: current_price_eur_mwh}
        station_res_counts : {station_id: {slot_idx: reservation_count}}
        """
        STATIONS = _get_stations()
        soc_band = 0 if soc < 0.30 else (1 if soc < 0.50 else 2)
        parts = [soc_band]
        for sid in ['station_A', 'station_B', 'station_C']:
            base  = STATIONS[sid]['base_price']
            price = station_prices.get(sid, base)
            price_level = (0 if price < base * 0.90
                           else 1 if price < base * 1.30
                           else 2)
            cap    = STATIONS[sid]['capacity']
            active = sum(station_res_counts.get(sid, {}).values())
            occ_level = (0 if active == 0
                         else 1 if active < cap * 0.6
                         else 2)
            parts.extend([price_level, occ_level])
        return tuple(parts)

    def select_action(self, state: tuple):
        """
        Epsilon-greedy action selection.

        Returns one of ACTIONS (station_id string or None).
        """
        if random.random() < self.epsilon:
            return random.choice(ACTIONS)
        q_vals = self._q(state)
        return ACTIONS[q_vals.index(max(q_vals))]

    def update(
        self,
        state:      tuple,
        action,
        reward:     float,
        next_state: tuple,
    ):
        """
        Standard Q-update:
            Q[s,a] ← Q[s,a] + α · (r + γ · max_a' Q[s',a'] − Q[s,a])

        Decays epsilon and records reward for the dashboard sparkline.
        """
        a_idx  = ACTIONS.index(action)
        q_vals = self._q(state)
        next_q = max(self._q(next_state))
        q_vals[a_idx] += ALPHA * (reward + GAMMA * next_q - q_vals[a_idx])

        self.epsilon = max(EPSILON_MIN, self.epsilon * EPSILON_DECAY)
        self.steps  += 1

        self.reward_history.append(round(reward, 3))
        if len(self.reward_history) > 200:
            self.reward_history.pop(0)

    def compute_reward(
        self,
        chosen_station,
        chosen_slots: list,
        station_prices: dict,
        soc: float,
        car_data: dict,
    ) -> float:
        """
        Immediate reward for a booking decision.

        Reward components:
          • Base:     (2.0 - price/base_price) × num_slots  → cheaper = higher
          • Travel:   -2 × travel_EUR  (penalise long trips)
          • Urgency:  +3.0 if soc < 0.30, +1.0 if soc < 0.50
          • No-book:  -5.0 if soc < 0.30 (critical), -1.0 otherwise
        """
        STATIONS = _get_stations()

        if not chosen_slots or chosen_station is None:
            return -5.0 if soc < 0.30 else -1.0

        base  = STATIONS[chosen_station]['base_price']
        price = station_prices.get(chosen_station, base)

        # Price quality: 1.0 = paying base, >1 = overpaying
        cost_ratio = price / base
        reward = (2.0 - cost_ratio) * len(chosen_slots)

        # Travel penalty (same formula as heuristic)
        car_x  = car_data.get('x', 5.0)
        car_y  = car_data.get('y', 5.0)
        stn    = STATIONS[chosen_station]
        dist   = math.sqrt((car_x - stn['x']) ** 2 + (car_y - stn['y']) ** 2)
        travel_eur = 2 * dist * 0.2 * base / 1000
        reward -= travel_eur * 2.0

        # Urgency bonus: reward the agent for getting a slot when SOC is low
        if soc < 0.30:
            reward += 3.0
        elif soc < 0.50:
            reward += 1.0

        return round(reward, 3)
