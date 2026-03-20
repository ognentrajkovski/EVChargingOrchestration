"""
Dynamic Bidding Price Producer
================================
Computes dynamic EV charging prices using a two-component algorithm:

  Component 1 — Instantaneous utilization (weight: 60%, reactive):
    Counts active chargers → maps to a price state:
      0–30% occupied  →  ×0.6  (off-peak)
      31–60% occupied →  ×1.0  (normal)
      61–100% occupied → ×1.8  (peak)

  Component 2 — Hourly profile (weight: 40%, predictive):
    Tracks how often each hour (0–23) historically was in "peak" state.
    After enough observations the price is nudged toward the expected level
    for that hour, allowing the system to raise prices preventively before
    a rush actually arrives.

  Final price = (instantaneous_price × 0.6) + (historical_price × 0.4)

Price examples (BASE=50 EUR/MWh, TOTAL=5 chargers, no history yet):
  0–1/5 active → 30.0 EUR/MWh   (off-peak)
  2–3/5 active → 50.0 EUR/MWh   (normal)
  4–5/5 active → 90.0 EUR/MWh   (peak)
"""

import json
import time
import threading
import os
import sys
from kafka import KafkaProducer, KafkaConsumer

# Add project root to path for ev_logger import
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
try:
    from ev_logger import log
    _log = lambda msg: log('ENERGY_PROD', msg)
except Exception:
    _log = lambda msg: print(f'[ENERGY_PROD] {msg}')

# ---------------------------------------------------------------------------
# Configuration — keep in sync with heuristic_scheduler.py constants
# ---------------------------------------------------------------------------
BOOTSTRAP_SERVERS    = os.environ.get('KAFKA_BOOTSTRAP_SERVERS', 'localhost:9092')
TOPIC_ENERGY         = 'energy_data'
TOPIC_COMMANDS       = 'charging_commands'

UPDATE_INTERVAL      = 1.25    # seconds per tick (matches car producer)
TOTAL_CHARGERS       = 5       # must match STATION_CAPACITY in scheduler
BASE_PRICE           = 50.0    # EUR/MWh at baseline (normal occupancy)

# Occupancy → price state thresholds (ratio = active / total)
# (upper_bound_inclusive, multiplier)
PRICE_LEVELS = [
    (0.30, 0.6),   # off-peak: ≤ 30% occupied → ×0.6 → 30 EUR/MWh
    (0.60, 1.0),   # normal:   ≤ 60% occupied → ×1.0 → 50 EUR/MWh
    (1.01, 1.8),   # peak:     > 60% occupied → ×1.8 → 90 EUR/MWh
]

# Weights for the two pricing components
INSTANT_WEIGHT  = 0.6
HISTORIC_WEIGHT = 0.4

# Minimum observations before the hourly profile influences the price
MIN_HISTORY_OBS = 4

# ---------------------------------------------------------------------------
# Shared state — updated by the command-consumer thread
# ---------------------------------------------------------------------------
active_chargers      = 0        # count of currently charging cars
active_chargers_lock = threading.Lock()
active_car_ids: set  = set()    # which car_ids are actively charging

sim_time_idx         = 48       # default to 12:00
sim_time_lock        = threading.Lock()

# Reservation data — updated from RESERVATION_STATUS messages
reservation_lock     = threading.Lock()
projected_prices     = []       # list of 96 floats (projected price per slot)
reservation_counts   = {}       # {str(slot_idx): count}

# Hourly profile — learns peak frequency per hour of day
# hour (0–23) → {'peak_count': int, 'total_count': int}
hourly_profile      = {h: {'peak_count': 0, 'total_count': 0} for h in range(24)}
hourly_profile_lock = threading.Lock()


def get_multiplier(active: int, total: int = TOTAL_CHARGERS) -> float:
    """Map current occupancy to the state-based price multiplier."""
    ratio = active / max(total, 1)
    for upper, mult in PRICE_LEVELS:
        if ratio <= upper:
            return mult
    return PRICE_LEVELS[-1][1]


def compute_price(active: int, hour: int, total: int = TOTAL_CHARGERS) -> tuple:
    """
    Two-component dynamic price:
      60% — instantaneous occupancy state (reactive)
      40% — hourly profile (predictive, learns over time)

    Compound boost: when BOTH components signal peak simultaneously,
    an extra ×1.3 is applied — being busy during a historically-busy
    hour costs more than being busy during a quiet hour.

    Returns (final_price, instant_price, hist_price, peak_freq_or_None)
    """
    instant_mult  = get_multiplier(active, total)
    instant_price = BASE_PRICE * instant_mult

    with hourly_profile_lock:
        profile   = hourly_profile[hour % 24]
        total_obs = profile['total_count']
        peak_obs  = profile['peak_count']

    if total_obs >= MIN_HISTORY_OBS:
        peak_freq  = peak_obs / total_obs
        off_mult   = PRICE_LEVELS[0][1]
        peak_mult  = PRICE_LEVELS[-1][1]
        hist_mult  = off_mult + (peak_mult - off_mult) * peak_freq
        hist_price = BASE_PRICE * hist_mult
    else:
        peak_freq  = None
        hist_price = instant_price  # mirror instant until enough history

    blended = instant_price * INSTANT_WEIGHT + hist_price * HISTORIC_WEIGHT

    # Compound boost: both NOW busy (peak state) AND historically peak hour
    instant_is_peak = instant_mult >= PRICE_LEVELS[-1][1]
    hist_is_peak    = peak_freq is not None and peak_freq >= 0.6
    compound        = 1.3 if (instant_is_peak and hist_is_peak) else 1.0

    return round(blended * compound, 4), round(instant_price, 4), round(hist_price, 4), peak_freq


def consume_commands():
    """
    Background thread: listens to Flink's charging_commands topic and tracks
    how many real (non-test) cars are currently in START_CHARGING state.
    This count is used to compute the dynamic price each tick.
    """
    global active_chargers, active_car_ids, sim_time_idx
    global projected_prices, reservation_counts

    consumer = KafkaConsumer(
        TOPIC_COMMANDS,
        bootstrap_servers=BOOTSTRAP_SERVERS,
        group_id='energy_producer_occupancy',
        value_deserializer=lambda x: json.loads(x.decode('utf-8')),
        auto_offset_reset='latest',
    )
    _log('Command consumer started — tracking charger occupancy')

    for message in consumer:
        cmd     = message.value

        # Handle RESERVATION_STATUS messages from Flink
        if cmd.get('type') == 'RESERVATION_STATUS':
            with reservation_lock:
                projected_prices = cmd.get('projected_prices', [])
                reservation_counts = cmd.get('reservation_counts', {})
            continue

        car_id  = cmd.get('car_id', '')
        action  = cmd.get('action', '')

        # Extract interval_idx from Flink commands to sync time
        with sim_time_lock:
            if 'interval_idx' in cmd and cmd['interval_idx'] is not None:
                sim_time_idx = cmd['interval_idx']

        with active_chargers_lock:
            if action == 'START_CHARGING':
                active_car_ids.add(car_id)
            elif action == 'STOP_CHARGING':
                active_car_ids.discard(car_id)
            active_chargers = len(active_car_ids)


def main():
    print('Dynamic Bidding Price Producer Starting...')
    print(f'  BASE_PRICE={BASE_PRICE} EUR/MWh | CHARGERS={TOTAL_CHARGERS} | WEIGHTS={INSTANT_WEIGHT:.0%} instant + {HISTORIC_WEIGHT:.0%} historic')
    _labels = ['off-peak', 'normal', 'peak']
    print(f'  PRICE STATES: ' + ' | '.join(
        f'{_labels[i]} ×{m}→{BASE_PRICE*m:.0f}€' for i, (_, m) in enumerate(PRICE_LEVELS)
    ))

    # Start the occupancy-tracking consumer thread
    t = threading.Thread(target=consume_commands, daemon=True)
    t.start()

    producer = KafkaProducer(
        bootstrap_servers=BOOTSTRAP_SERVERS,
        value_serializer=lambda v: json.dumps(v).encode('utf-8'),
    )
    _log(f'Connected to Kafka at {BOOTSTRAP_SERVERS}')

    tick = 0
    try:
        while True:
            with active_chargers_lock:
                n_active = active_chargers

            with sim_time_lock:
                s_idx = sim_time_idx

            # Derive simulated hour from slot index (each slot = 15 min)
            hour = (s_idx * 15) // 60

            price, instant_price, hist_price, peak_freq = compute_price(n_active, hour)

            # Update hourly profile with current observation
            is_peak = get_multiplier(n_active) >= PRICE_LEVELS[-1][1]
            with hourly_profile_lock:
                hourly_profile[hour % 24]['total_count'] += 1
                if is_peak:
                    hourly_profile[hour % 24]['peak_count'] += 1
                profile_snapshot = {
                    str(h): dict(v) for h, v in hourly_profile.items()
                }

            with reservation_lock:
                proj_prices = list(projected_prices) if projected_prices else []
                res_counts = dict(reservation_counts) if reservation_counts else {}

            record = {
                'type':               'DYNAMIC_PRICE',
                'active_chargers':    n_active,
                'total_chargers':     TOTAL_CHARGERS,
                'base_price':         BASE_PRICE,
                'current_price':      price,
                'instant_price':      instant_price,
                'hist_price':         hist_price,
                'peak_freq':          round(peak_freq, 3) if peak_freq is not None else None,
                'compound_boost':     price > (instant_price * INSTANT_WEIGHT + hist_price * HISTORIC_WEIGHT) * 1.01,
                'price_state':        ('peak' if is_peak else
                                       'normal' if get_multiplier(n_active) >= 1.0 else
                                       'off_peak'),
                'timestamp':          time.time(),
                'interval_idx':       s_idx,
                'sim_hour':           hour,
                'projected_prices':   proj_prices,
                'reservation_counts': res_counts,
                'hourly_profile':     profile_snapshot,
            }
            producer.send(TOPIC_ENERGY, record)

            if tick % 20 == 0:  # log every ~25s to avoid spam
                state = record['price_state']
                boost_tag = ' [COMPOUND BOOST]' if record['compound_boost'] else ''
                _log(
                    f'tick={tick} | hour={hour:02d}:xx | active={n_active}/{TOTAL_CHARGERS} '
                    f'| state={state} | instant={instant_price:.1f} hist={hist_price:.1f} '
                    f'→ final={price:.2f} EUR/MWh{boost_tag}'
                )
            tick += 1
            time.sleep(UPDATE_INTERVAL)

    except KeyboardInterrupt:
        print('\\nEnergy producer stopped.')
    finally:
        producer.close()


if __name__ == '__main__':
    main()
