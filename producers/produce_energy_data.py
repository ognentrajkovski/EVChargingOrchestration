"""
Dynamic Bidding Price Producer  (multi-station)
================================================
Computes independent dynamic prices per charging station using a two-component
algorithm per station:

  Component 1 — Instantaneous utilisation (weight 60%, reactive):
    Counts active chargers at THIS station → price state (off-peak / normal / peak).

  Component 2 — Hourly profile (weight 40%, predictive):
    Learns which hours are historically busy AT THIS STATION and raises prices
    proactively before a rush arrives.

  Final = (instant × 0.6) + (historic × 0.4)  + optional ×1.3 compound boost

Publishes to `energy_data` as:
  {
    "type": "DYNAMIC_PRICE",
    "stations": {
      "station_A": { current_price, active_chargers, capacity, base_price, ... },
      ...
    },
    "interval_idx": int,
    "timestamp": float,
    "hourly_profile": { ... }   # aggregate across all stations for dashboard
  }
"""

import json
import time
import threading
import os
import sys

from kafka import KafkaProducer, KafkaConsumer

# Add project root to path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
try:
    from ev_logger import log
    _log = lambda msg: log('ENERGY_PROD', msg)
except Exception:
    _log = lambda msg: print(f'[ENERGY_PROD] {msg}')

from config.stations import STATIONS, PRICE_LEVELS

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------
BOOTSTRAP_SERVERS = os.environ.get('KAFKA_BOOTSTRAP_SERVERS', 'localhost:9092')
TOPIC_ENERGY      = 'energy_data'
TOPIC_COMMANDS    = 'charging_commands'
UPDATE_INTERVAL   = 1.25   # seconds per tick

INSTANT_WEIGHT  = 0.6
HISTORIC_WEIGHT = 0.4
MIN_HISTORY_OBS = 4


# ---------------------------------------------------------------------------
# Shared state — updated by the command-consumer thread
# ---------------------------------------------------------------------------
# Per-station active car sets  {station_id: set(car_ids)}
station_active_car_ids: dict = {sid: set() for sid in STATIONS}
station_lock = threading.Lock()

sim_time_idx      = 48   # default 12:00
sim_time_lock     = threading.Lock()

# Reservation status from Flink  {station_id: {slot_idx_str: count}}
reservation_lock  = threading.Lock()
res_counts_per_station: dict = {}
proj_prices_per_station: dict = {}

# Per-station hourly learning profiles  {station_id: {hour: {peak_count, total_count}}}
hourly_profiles: dict = {
    sid: {h: {'peak_count': 0, 'total_count': 0} for h in range(24)}
    for sid in STATIONS
}
hourly_lock = threading.Lock()


# ---------------------------------------------------------------------------
# Pricing helpers
# ---------------------------------------------------------------------------

def _get_multiplier(active: int, capacity: int) -> float:
    ratio = active / max(capacity, 1)
    for upper, mult in PRICE_LEVELS:
        if ratio <= upper:
            return mult
    return PRICE_LEVELS[-1][1]


def compute_station_price(
    active: int,
    hour: int,
    capacity: int,
    base_price: float,
    station_id: str,
) -> dict:
    """
    Compute the dynamic price for one station and return a data dict.
    """
    instant_mult  = _get_multiplier(active, capacity)
    instant_price = base_price * instant_mult

    with hourly_lock:
        profile   = hourly_profiles[station_id][hour % 24]
        total_obs = profile['total_count']
        peak_obs  = profile['peak_count']

    if total_obs >= MIN_HISTORY_OBS:
        peak_freq  = peak_obs / total_obs
        off_mult   = PRICE_LEVELS[0][1]
        peak_mult  = PRICE_LEVELS[-1][1]
        hist_mult  = off_mult + (peak_mult - off_mult) * peak_freq
        hist_price = base_price * hist_mult
    else:
        peak_freq  = None
        hist_price = instant_price

    blended = instant_price * INSTANT_WEIGHT + hist_price * HISTORIC_WEIGHT

    instant_is_peak = instant_mult >= PRICE_LEVELS[-1][1]
    hist_is_peak    = peak_freq is not None and peak_freq >= 0.6
    compound        = 1.3 if (instant_is_peak and hist_is_peak) else 1.0

    final_price = round(blended * compound, 4)

    price_state = (
        'peak'     if instant_mult >= PRICE_LEVELS[-1][1] else
        'normal'   if instant_mult >= PRICE_LEVELS[1][1]  else
        'off_peak'
    )

    return {
        'current_price':   final_price,
        'instant_price':   round(instant_price, 4),
        'hist_price':      round(hist_price, 4),
        'peak_freq':       round(peak_freq, 3) if peak_freq is not None else None,
        'compound_boost':  compound > 1.0,
        'price_state':     price_state,
        'active_chargers': active,
        'capacity':        capacity,
        'base_price':      base_price,
    }


def _update_hourly_profile(station_id: str, hour: int, is_peak: bool):
    with hourly_lock:
        hourly_profiles[station_id][hour % 24]['total_count'] += 1
        if is_peak:
            hourly_profiles[station_id][hour % 24]['peak_count'] += 1


def _snapshot_hourly_profile():
    """Aggregate hourly profile across all stations for dashboard display."""
    with hourly_lock:
        aggregate = {}
        for h in range(24):
            total = sum(hourly_profiles[sid][h]['total_count'] for sid in STATIONS)
            peaks = sum(hourly_profiles[sid][h]['peak_count'] for sid in STATIONS)
            aggregate[str(h)] = {'peak_count': peaks, 'total_count': total}
        return aggregate


# ---------------------------------------------------------------------------
# Command consumer
# ---------------------------------------------------------------------------

def consume_commands():
    """
    Background thread: listens to charging_commands and tracks active chargers
    per station. Also receives RESERVATION_STATUS from Flink.
    """
    global sim_time_idx

    consumer = KafkaConsumer(
        TOPIC_COMMANDS,
        bootstrap_servers=BOOTSTRAP_SERVERS,
        group_id='energy_producer_occupancy',
        value_deserializer=lambda x: json.loads(x.decode('utf-8')),
        auto_offset_reset='latest',
    )
    _log('Command consumer started — tracking per-station charger occupancy')

    for message in consumer:
        cmd = message.value

        # ── RESERVATION_STATUS from Flink ──────────────────────────────
        if cmd.get('type') == 'RESERVATION_STATUS':
            per_stn = cmd.get('reservation_counts_per_station', {})
            proj    = cmd.get('projected_prices', [])  # aggregate backward-compat
            with reservation_lock:
                res_counts_per_station.clear()
                res_counts_per_station.update(per_stn)
                # Also store per-station projected prices if present
                for sid, sdata in cmd.get('stations', {}).items():
                    proj_prices_per_station[sid] = sdata.get('projected_prices', [])
            continue

        car_id = cmd.get('car_id', '')
        action = cmd.get('action', '')
        sid    = cmd.get('station_id')   # NEW field from multi-station Flink

        # Sync simulation clock
        with sim_time_lock:
            if cmd.get('interval_idx') is not None:
                sim_time_idx = cmd['interval_idx']

        with station_lock:
            if action == 'START_CHARGING' and sid and sid in station_active_car_ids:
                station_active_car_ids[sid].add(car_id)
            elif action == 'STOP_CHARGING':
                # Remove from whichever station this car was at
                for s in station_active_car_ids:
                    station_active_car_ids[s].discard(car_id)


# ---------------------------------------------------------------------------
# Main loop
# ---------------------------------------------------------------------------

def main():
    print('Multi-Station Dynamic Price Producer Starting...')
    for sid, stn in STATIONS.items():
        print(f"  {sid} ({stn['name']}): base={stn['base_price']} EUR/MWh | capacity={stn['capacity']}")

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
            with sim_time_lock:
                s_idx = sim_time_idx
            hour = (s_idx * 15) // 60

            # ── Compute price per station ──────────────────────────────
            with station_lock:
                active_snapshot = {sid: len(ids) for sid, ids in station_active_car_ids.items()}

            stations_payload = {}
            for sid, stn in STATIONS.items():
                n_active   = active_snapshot.get(sid, 0)
                sdata      = compute_station_price(
                    n_active, hour, stn['capacity'], stn['base_price'], sid)
                _update_hourly_profile(sid, hour, sdata['price_state'] == 'peak')
                # Embed this station's own learned hourly profile so the dashboard
                # can show per-station peak-frequency charts.
                with hourly_lock:
                    sdata['hourly_profile'] = {
                        str(h): dict(hourly_profiles[sid][h]) for h in range(24)
                    }
                stations_payload[sid] = sdata

            profile_snapshot = _snapshot_hourly_profile()

            with reservation_lock:
                flat_res  = dict(res_counts_per_station)
                flat_proj = dict(proj_prices_per_station)

            record = {
                'type':          'DYNAMIC_PRICE',
                'stations':      stations_payload,
                'interval_idx':  s_idx,
                'sim_hour':      hour,
                'timestamp':     time.time(),
                'hourly_profile': profile_snapshot,
                # Backward-compat aggregate fields (first station's data)
                'current_price': stations_payload.get('station_A', {}).get('current_price', 50.0),
                'active_chargers': sum(active_snapshot.values()),
                'total_chargers':  sum(s['capacity'] for s in STATIONS.values()),
                'reservation_counts_per_station': flat_res,
            }
            producer.send(TOPIC_ENERGY, record)

            if tick % 20 == 0:
                parts = [
                    f"{sid}={v['current_price']:.1f}€({v['active_chargers']}/{v['capacity']})"
                    for sid, v in stations_payload.items()
                ]
                _log(f'tick={tick} | hour={hour:02d} | ' + ' | '.join(parts))

            tick += 1
            time.sleep(UPDATE_INTERVAL)

    except KeyboardInterrupt:
        print('\nEnergy producer stopped.')
    finally:
        producer.close()


if __name__ == '__main__':
    main()
