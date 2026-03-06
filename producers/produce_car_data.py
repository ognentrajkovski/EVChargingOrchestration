import json
import time
import random
import threading
from kafka import KafkaProducer, KafkaConsumer
import sys, os
# producers/ -> go up one level to project root
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from ev_logger import log, log_error

# Configuration
NUM_CARS = 1
UPDATE_INTERVAL = 1.25
TOPIC_REAL = "cars_real"
TOPIC_COMMANDS = "charging_commands"
BOOTSTRAP_SERVERS = os.environ.get('KAFKA_BOOTSTRAP_SERVERS', 'localhost:9092')
BATTERY_CAPACITY_KWH = 60.0

class Car:
    def __init__(self, car_id, group_name, start_battery, priority, k):
        self.id = car_id
        self.group = group_name
        self.battery_level = start_battery
        self.priority = priority
        self.k = k
        self.is_charging = False
        self.is_parked = False  # Cars start driving; Flink will send START_CHARGING when a plan slot arrives

    def update(self):
        if self.is_charging:
            # Flink controls charging — battery updated via START_CHARGING commands
            log('CAR_UPDATE', f"{self.id} | CHARGING | batt={self.battery_level:.1f}%")
        elif not self.is_parked:
            # Driving — discharge continuously, Flink decides when to call back for charging
            self.battery_level = max(0.0, self.battery_level - self.k)
            log('CAR_UPDATE', f"{self.id} | DRIVING | batt={self.battery_level:.1f}%")

    def to_dict(self):
        return {
            "id": self.id,
            "group": self.group,
            "current_battery_kwh": (self.battery_level / 100.0) * BATTERY_CAPACITY_KWH,
            "current_soc": self.battery_level / 100.0,
            "priority": self.priority,
            "plugged_in": self.is_parked,  # Important for Flink
            "timestamp": time.time()
        }

# Global dictionary to access cars from consumer thread
cars_map = {}
cars_lock = threading.Lock()  # Protects cars_map and Car field writes across threads

def consume_commands():
    """Background thread to listen for charging commands/updates from Flink."""
    consumer = KafkaConsumer(
        TOPIC_COMMANDS,
        bootstrap_servers=BOOTSTRAP_SERVERS,
        group_id='car_producer_feedback',
        value_deserializer=lambda x: json.loads(x.decode('utf-8'))
    )

    print("Command Consumer Started...")
    for message in consumer:
        cmd = message.value
        car_id  = cmd.get('car_id')
        action  = cmd.get('action')
        new_soc = cmd.get('new_soc')

        with cars_lock:
            if car_id in cars_map:
                car = cars_map[car_id]
                if action == 'START_CHARGING':
                    log('CAR_PROD', f"{car_id} | START_CHARGING | new_soc={new_soc} | batt_before={car.battery_level:.1f}%")
                    # Accept Flink's SOC only if it's higher — Flink accumulates energy
                    # correctly when it owns the state (flink_managed=True), so its value
                    # should be trusted and never lowered by a stale sensor reading.
                    if new_soc is not None and (new_soc * 100.0) > car.battery_level:
                        car.battery_level = new_soc * 100.0
                    car.is_charging = True
                    car.is_parked   = True
                    log('CAR_PROD', f"{car_id} | after START | batt={car.battery_level:.1f}%")
                elif action == 'STOP_CHARGING':
                    log('CAR_PROD', f"{car_id} | STOP_CHARGING | new_soc={new_soc} | batt_before={car.battery_level:.1f}%")
                    # Never let STOP lower the battery — Flink's new_soc can be stale
                    if new_soc is not None and (new_soc * 100.0) > car.battery_level:
                        car.battery_level = new_soc * 100.0
                    car.is_charging = False
                    car.is_parked = False  # Car leaves station and drives — Flink will call it back when needed
                    log('CAR_PROD', f"{car_id} | after STOP | batt={car.battery_level:.1f}% | driving")

def main():
    print(f"Starting Car Producer with Delegation Logic...")

    producer = KafkaProducer(
        bootstrap_servers=BOOTSTRAP_SERVERS,
        value_serializer=lambda v: json.dumps(v).encode('utf-8')
    )

    # Initialize cars — real group + identical test group with same parameters
    for i in range(1, NUM_CARS + 1):
        start_battery = random.uniform(85.0, 100.0)
        priority = random.randint(1, 5)
        discharge_k = random.uniform(0.4, 1.0)  # % per tick (2.5s)

        cars_map[f"car_{i}"]      = Car(f"car_{i}",      "real", start_battery, priority, discharge_k)
        cars_map[f"car_{i}_test"] = Car(f"car_{i}_test", "test", start_battery, priority, discharge_k)

    print(f"Initialized {NUM_CARS} real + {NUM_CARS} test cars (identical parameters).")

    # Start Consumer Thread
    t = threading.Thread(target=consume_commands, daemon=True)
    t.start()

    try:
        while True:
            # Snapshot cars under lock, then update and send outside lock
            with cars_lock:
                items = list(cars_map.items())
            for car_id, car in items:
                with cars_lock:
                    car.update()
                    data = car.to_dict()
                producer.send(TOPIC_REAL, data)

            producer.flush()
            time.sleep(UPDATE_INTERVAL)

    except KeyboardInterrupt:
        print("\nStopping car producer...")
    finally:
        producer.close()

if __name__ == "__main__":
    main()