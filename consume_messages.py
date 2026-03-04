from kafka import KafkaConsumer
import json

bootstrap_servers = 'localhost:9092'
# Updated to match current project topics
topics = ['energy_data', 'cars_real', 'charging_commands']

consumer = KafkaConsumer(
    *topics,
    bootstrap_servers=bootstrap_servers,
    group_id='debug_consumer_group',
    auto_offset_reset='latest',
    enable_auto_commit=False
)

print(f"Listening to topics: {topics} on {bootstrap_servers}...")

try:
    for message in consumer:
        topic = message.topic
        value = message.value.decode('utf-8')

        try:
            data = json.loads(value)
            pretty = json.dumps(data, indent=2, ensure_ascii=False)
            print(f"[{topic}] {pretty}")
        except Exception:
            print(f"[{topic}] {value}")

except KeyboardInterrupt:
    print("Stopping consumer...")
finally:
    consumer.close()
