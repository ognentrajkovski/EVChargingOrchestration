import json
import time
import random
import threading
from kafka import KafkaProducer, KafkaConsumer

# Configuration
NUM_CARS = 5
UPDATE_INTERVAL = 2.5
TOPIC_REAL = "cars_real"
TOPIC_COMMANDS = "charging_commands"
BOOTSTRAP_SERVERS = 'localhost:9092'
BATTERY_CAPACITY_KWH = 60.0

class Car:
    def __init__(self, car_id, group_name, start_battery, priority, k):
        self.id = car_id
        self.group = group_name
        self.battery_level = start_battery
        self.priority = priority
        self.k = k
        self.is_charging = False 
        self.is_parked = False # Controlled entirely by Flink
    
    def update(self):
        # Simulate discharging (driving) only if Flink hasn't parked/charged us
        if not self.is_parked:
            self.battery_level = max(0.0, self.battery_level - self.k)

    def set_battery(self, new_soc, is_charging):
        if new_soc is not None:
            self.battery_level = new_soc * 100.0 
            
        self.is_charging = is_charging
        self.is_parked = is_charging # Parked if charging, driving if not
            
    def to_dict(self):
        return {
            "id": self.id,
            "group": self.group,
            "current_battery_kwh": (self.battery_level / 100.0) * BATTERY_CAPACITY_KWH,
            "current_soc": self.battery_level / 100.0,
            "priority": self.priority,
            "plugged_in": self.is_parked, # Important for Flink
            "timestamp": time.time()
        }

# Global dictionary to access cars from consumer thread
cars_map = {}

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
        car_id = cmd.get('car_id')
        action = cmd.get('action')
        new_soc = cmd.get('new_soc')
        
        if car_id in cars_map:
            car = cars_map[car_id]
            if action == 'START_CHARGING':
                # If it wasn't already charging, announce it plugged in
                if not car.is_parked:
                    print(f"[FLINK COMMAND] {car_id} -> 🔌 PLUGGED IN. Starting charge at {car.battery_level:.1f}%")
                car.set_battery(new_soc, True)
            elif action == 'STOP_CHARGING':
                # Only print if the car was previously charging/parked
                if car.is_parked:
                    print(f"[FLINK COMMAND] {car_id} -> 🛑 STOP CHARGING. Disconnected at {new_soc*100 if new_soc else car.battery_level:.1f}%")
                car.set_battery(new_soc, False)

def main():
    print(f"Starting Car Producer with Flink-Only Logic...")
    
    producer = KafkaProducer(
        bootstrap_servers=BOOTSTRAP_SERVERS,
        value_serializer=lambda v: json.dumps(v).encode('utf-8')
    )

    # Initialize cars
    for i in range(1, NUM_CARS + 1):
        # Force all cars to start with >80% battery
        start_battery = random.uniform(85.0, 100.0)
        priority = random.randint(1, 5)
        # Average battery life ~360s. Interval 2.5s. K ~ 0.7%
        discharge_k = random.uniform(0.4, 1.0)

        # Create Real Car
        car = Car(f"car_{i}", "real", start_battery, priority, discharge_k)
        cars_map[f"car_{i}"] = car

    print(f"Initialized {NUM_CARS} cars.")
    
    # Start Consumer Thread
    t = threading.Thread(target=consume_commands, daemon=True)
    t.start()

    try:
        while True:
            # Update and Send Real Group
            for car_id, car in cars_map.items():
                car.update()
                producer.send(TOPIC_REAL, car.to_dict())

            producer.flush()
            time.sleep(UPDATE_INTERVAL)

    except KeyboardInterrupt:
        print("\nStopping car producer...")
    finally:
        producer.close()

if __name__ == "__main__":
    main()