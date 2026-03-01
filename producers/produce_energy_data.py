import time
import json
import pandas as pd
from kafka import KafkaProducer

# Configuration
BOOTSTRAP_SERVERS = 'localhost:9092'
TOPIC_NAME = 'energy_data'
CSV_FILE_PATH = '../data/Austria_Energy_Reports.csv'
BATCH_SIZE = 96
INTERVAL_SECONDS = 120 # 1 simulated day = 120 real seconds
TIME_TO_1300 = 65 # 13:00 is exactly 65 seconds into the day

def create_producer():
    return KafkaProducer(
        bootstrap_servers=BOOTSTRAP_SERVERS,
        value_serializer=lambda v: json.dumps(v).encode('utf-8')
    )

def send_tomorrow_batch(producer, df, start_index):
    total_rows = len(df)
    end_index = start_index + 96
    
    if end_index <= total_rows:
        batch = df.iloc[start_index:end_index]
    else:
        # Wrap around
        remaining = total_rows - start_index
        batch_part1 = df.iloc[start_index:total_rows]
        batch_part2 = df.iloc[0 : (96 - remaining)]
        batch = pd.concat([batch_part1, batch_part2])
        
    print(f"Sending 96 prices for TOMORROW...")
    for _, row in batch.iterrows():
        record = row.to_dict()
        record['sent_at'] = time.time()
        # Explicitly mark these as Tomorrow's data
        producer.send(TOPIC_NAME, record)
        
    producer.flush()
    return end_index % total_rows

def main():
    print("Energy Market Simulator Started at 00:00.")
    print("Waiting until 13:00 to release TOMORROW'S prices...")
    
    try:
        df = pd.read_csv(CSV_FILE_PATH, sep=';')
    except FileNotFoundError:
        print(f"Error: File {CSV_FILE_PATH} not found.")
        return

    producer = create_producer()
    current_index = 0
    
    try:
        # Loop forever
        while True:
            # 1. Wait until 13:00
            time.sleep(TIME_TO_1300)
            
            # 2. Market Clearing at 13:00: Send ONLY Tomorrow's prices (96 rows)
            print("\n--- Market Cleared (13:00) ---")
            current_index = send_tomorrow_batch(producer, df, current_index)
            
            # 3. Wait for the rest of the day (55s) to reach 00:00 of the next day
            # Then wait another 65s to reach 13:00 of the next day.
            # Total wait between 13:00 clearings = 120s
            time.sleep(INTERVAL_SECONDS - TIME_TO_1300)

    except KeyboardInterrupt:
        print("Producer stopped.")
    finally:
        producer.close()

if __name__ == '__main__':
    main()