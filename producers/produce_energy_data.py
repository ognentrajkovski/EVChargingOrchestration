import time
import json
import pandas as pd
import os
import sys
from kafka import KafkaProducer, KafkaConsumer
import sys
# producers/ -> go up one level to project root
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
try:
    from ev_logger import log
    _log = lambda msg: log('ENERGY_PROD', msg)
except Exception as e:
    _log = lambda msg: print(f'[ENERGY_PROD] {msg}')

# Configuration
BOOTSTRAP_SERVERS = os.environ.get('KAFKA_BOOTSTRAP_SERVERS', 'localhost:9092')
TOPIC_NAME = 'energy_data'
TOPIC_COMMANDS = 'charging_commands'

# --- ROBUST PATH FIX ---
# This ensures we find the file whether you run from 'root' or 'producers/'
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
CSV_FILE_PATH = os.path.join(SCRIPT_DIR, '../data/Austria_Energy_Reports.csv')

# Time settings: 1 real second = 12 sim minutes → full sim day = 120 real seconds
# 13:00 = interval 52 (52 × 15 sim-min = 780 sim-min = 13h)
# NOTE: Flink's processing timer on ARM64 Docker fires at ~1.77s/interval (not 1.25s),
# so one sim day = 96 × 1.77 ≈ 170 real seconds. Measured from log:
#   batch@13:00 → midnight = 78s / 44 intervals = 1.77s/interval.
INTERVAL_IDX_1300    = 52     # sim interval index for 13:00
SECS_PER_INTERVAL    = 1.786  # measured: 50s extra → 28 interval shift → 1.786s/interval
INTERVAL_SECONDS     = 159    # 120s→7:00, 170s→14:00, 163s→13:30, fine-tuned -4s→13:00
FALLBACK_DELAY       = 5      # fallback if Flink idx can't be read


def get_flink_interval_idx():
    """
    Read the FIRST charging_command from Flink (earliest offset) to find the
    starting sim interval index. Used only at startup, when the topic is fresh.
    Returns the interval_idx or None on failure.
    """
    try:
        consumer = KafkaConsumer(
            TOPIC_COMMANDS,
            bootstrap_servers=BOOTSTRAP_SERVERS,
            value_deserializer=lambda x: json.loads(x.decode('utf-8')),
            auto_offset_reset='earliest',
            enable_auto_commit=False,
            group_id=f'energy_sync_{int(time.time())}',
            consumer_timeout_ms=30000,
        )
        for msg in consumer:
            idx = msg.value.get('interval_idx')
            if idx is not None:
                consumer.close()
                return idx
        consumer.close()
    except Exception as e:
        print(f"⚠️  Could not read Flink idx: {e}")
    return None


def create_producer():
    try:
        producer = KafkaProducer(
            bootstrap_servers=BOOTSTRAP_SERVERS,
            value_serializer=lambda v: json.dumps(v).encode('utf-8')
        )
        print(f"✅ Connected to Kafka at {BOOTSTRAP_SERVERS}")
        return producer
    except Exception as e:
        print(f"❌ Failed to connect to Kafka: {e}")
        sys.exit(1)


def send_batch(producer, df, start_index, label="TOMORROW"):
    total_rows = len(df)
    end_index = start_index + 96

    if end_index <= total_rows:
        batch = df.iloc[start_index:end_index]
    else:
        remaining = total_rows - start_index
        batch_part1 = df.iloc[start_index:total_rows]
        batch_part2 = df.iloc[0: (96 - remaining)]
        batch = pd.concat([batch_part1, batch_part2])

    print(f"📤 Sending 96 prices for {label}...")
    prices_sent = []
    for _, row in batch.iterrows():
        record = row.to_dict()
        record['sent_at'] = time.time()
        record['type'] = label
        producer.send(TOPIC_NAME, record)
        prices_sent.append(record.get('AT_price_day_ahead', 0))

    producer.flush()
    _log(f"BATCH_SENT | label={label} | count={len(prices_sent)} | min={min(prices_sent):.1f} | max={max(prices_sent):.1f} | avg={sum(prices_sent)/len(prices_sent):.1f} | start_idx={start_index}")
    return end_index % total_rows


def main():
    print("🚀 Energy Market Simulator Starting...")
    print(f"📂 Looking for data at: {os.path.abspath(CSV_FILE_PATH)}")

    try:
        df = pd.read_csv(CSV_FILE_PATH, sep=';')
        print(f"✅ Loaded CSV with {len(df)} rows.")
        if 'AT_price_day_ahead' not in df.columns:
            print("❌ ERROR: Column 'AT_price_day_ahead' not found in CSV!")
            print(f"Found columns: {df.columns.tolist()}")
            return
    except FileNotFoundError:
        print(f"❌ CRITICAL ERROR: File not found at {CSV_FILE_PATH}")
        print("Please check that 'data/Austria_Energy_Reports.csv' exists.")
        return

    producer = create_producer()
    current_index = 0

    # --- Align with Flink's sim clock ---
    # Instead of a fixed delay, read Flink's current interval_idx and compute
    # exactly how many real seconds remain until sim 13:00 (idx=52).
    # This guarantees the first batch arrives at 13:00 regardless of Docker startup time.
    print("⏳ Reading Flink's current sim clock from charging_commands...")
    flink_idx = get_flink_interval_idx()

    if flink_idx is not None:
        # (52 - idx) % 96 gives intervals until next 13:00 (wraps to next day if past 13:00)
        intervals_to_wait = (INTERVAL_IDX_1300 - flink_idx) % 96
        wait_secs = intervals_to_wait * SECS_PER_INTERVAL
        sim_now_h = (flink_idx * 15) // 60
        sim_now_m = (flink_idx * 15) % 60
        print(f"  Flink sim clock: {sim_now_h:02d}:{sim_now_m:02d} (idx={flink_idx})")
        print(f"  Waiting {wait_secs:.1f}s ({intervals_to_wait} intervals) for 13:00 market clearing...")
        _log(f"CLOCK_SYNC | flink_idx={flink_idx} | wait={wait_secs:.1f}s")
        time.sleep(wait_secs)
    else:
        print(f"⚠️  Could not read Flink clock — using fallback {FALLBACK_DELAY}s delay")
        time.sleep(FALLBACK_DELAY)

    print("\n--- Market Cleared (13:00) — sending Tomorrow's Prices ---")
    _log("MARKET_CLEAR 13:00 — sending TOMORROW")
    current_index = send_batch(producer, df, current_index, label="TOMORROW")
    _log(f"TOMORROW sent. next_index={current_index}")

    try:
        while True:
            print(f"⏳ Waiting {INTERVAL_SECONDS}s for next Market Clearing (13:00)...")
            time.sleep(INTERVAL_SECONDS)

            print("\n--- Market Cleared (13:00) ---")
            _log(f"MARKET_CLEAR — sending new TOMORROW batch")
            current_index = send_batch(producer, df, current_index, label="TOMORROW")
            _log(f"TOMORROW sent. next_index={current_index}")

    except KeyboardInterrupt:
        print("Producer stopped.")
    finally:
        producer.close()


if __name__ == '__main__':
    main()
