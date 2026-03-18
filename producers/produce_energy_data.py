"""
Dynamic Bidding Price Producer
================================
Replaces the day-ahead CSV price streamer with a real-time occupancy-based
price broadcaster.

Every tick this producer:
  1. Counts how many chargers are currently active (from Flink's commands).
  2. Computes the current dynamic price using the formula:
       price = BASE_PRICE * (1 + PRICE_COEFFICIENT * active / total)
  3. Publishes the result to the 'energy_data' Kafka topic so Flink can
     use it in the per-tick charging decision.

Price examples (BASE=50, COEFF=1.5, TOTAL=5):
  0/5 active → 50.0 EUR/MWh   (baseline)
  1/5 active → 65.0 EUR/MWh
  2/5 active → 80.0 EUR/MWh
  3/5 active → 95.0 EUR/MWh
  4/5 active → 110.0 EUR/MWh
  5/5 active → 125.0 EUR/MWh  (2.5× baseline, full occupancy)
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
BASE_PRICE           = 50.0    # EUR/MWh at 0 occupancy
PRICE_COEFFICIENT    = 1.5     # price multiplier slope (see formula above)

# ---------------------------------------------------------------------------
# Shared state — updated by the command-consumer thread
# ---------------------------------------------------------------------------
active_chargers      = 0        # count of currently charging cars
active_chargers_lock = threading.Lock()
active_car_ids: set  = set()    # which car_ids are actively charging

sim_time_idx         = 48       # default to 12:00
sim_time_lock        = threading.Lock()


def compute_price(active: int, total: int = TOTAL_CHARGERS) -> float:
    """Dynamic price based on charger occupancy."""
    occupancy_ratio = active / max(total, 1)
    return BASE_PRICE * (1.0 + PRICE_COEFFICIENT * occupancy_ratio)


def consume_commands():
    """
    Background thread: listens to Flink's charging_commands topic and tracks
    how many real (non-test) cars are currently in START_CHARGING state.
    This count is used to compute the dynamic price each tick.
    """
    global active_chargers, active_car_ids, sim_time_idx

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
        car_id  = cmd.get('car_id', '')
        action  = cmd.get('action', '')

        # Extract interval_idx from Flink commands to sync time
        with sim_time_lock:
            if 'interval_idx' in cmd and cmd['interval_idx'] is not None:
                sim_time_idx = cmd['interval_idx']

        # Only count real cars (ignore _test cars in the occupancy tally)
        if car_id.endswith('_test'):
            continue

        with active_chargers_lock:
            if action == 'START_CHARGING':
                active_car_ids.add(car_id)
            elif action == 'STOP_CHARGING':
                active_car_ids.discard(car_id)
            active_chargers = len(active_car_ids)


def main():
    print('Dynamic Bidding Price Producer Starting...')
    print(f'  BASE_PRICE={BASE_PRICE} EUR/MWh | COEFFICIENT={PRICE_COEFFICIENT} | CHARGERS={TOTAL_CHARGERS}')

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

            price = compute_price(n_active)
            with sim_time_lock:
                s_idx = sim_time_idx

            record = {
                'type':            'DYNAMIC_PRICE',
                'active_chargers': n_active,
                'total_chargers':  TOTAL_CHARGERS,
                'base_price':      BASE_PRICE,
                'current_price':   round(price, 4),
                'timestamp':       time.time(),
                'interval_idx':    s_idx,
            }
            producer.send(TOPIC_ENERGY, record)

            if tick % 20 == 0:  # log every ~25s to avoid spam
                _log(
                    f'tick={tick} | active={n_active}/{TOTAL_CHARGERS} '
                    f'| price={price:.2f} EUR/MWh'
                )
            tick += 1
            time.sleep(UPDATE_INTERVAL)

    except KeyboardInterrupt:
        print('\\nEnergy producer stopped.')
    finally:
        producer.close()


if __name__ == '__main__':
    main()
