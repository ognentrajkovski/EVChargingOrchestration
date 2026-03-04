import time
import json
import pandas as pd
import os
import sys
from kafka import KafkaProducer
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

# --- ROBUST PATH FIX ---
# This ensures we find the file whether you run from 'root' or 'producers/'
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
CSV_FILE_PATH = os.path.join(SCRIPT_DIR, '../data/Austria_Energy_Reports.csv')

# Time settings: 1 real second = 6 sim minutes → full sim day = 240 real seconds
# Clock starts at 12:00 → 13:00 is 60 sim-min away = 10 real seconds
# No TODAY prices at startup — only TOMORROW prices from 13:00 onward
TIME_TO_1300 = 5        # real seconds from startup (12:00) to first market clearing (13:00)
INTERVAL_SECONDS = 120   # real seconds between each new TOMORROW batch


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
        # Sanity check columns
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

    # --- Sim clock starts at 12:00 ---
    # No TODAY prices — from 12:00 to midnight only TOMORROW prices are available.
    # Wait for 13:00 market clearing (10 real seconds = 60 sim minutes from 12:00)
    print(f"⏳ Clock at 12:00 — waiting {TIME_TO_1300}s for 13:00 market clearing...")
    time.sleep(TIME_TO_1300)
    print("\n--- Market Cleared (13:00) — sending Tomorrow's Prices ---")
    _log("MARKET_CLEAR 13:00 — sending TOMORROW")
    current_index = send_batch(producer, df, current_index, label="TOMORROW")
    _log(f"TOMORROW sent. next_index={current_index}")

    try:
        while True:
            # Wait a full simulation day before the next 13:00 market clearing
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