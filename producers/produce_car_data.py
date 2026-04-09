import json
import time
import random
import math
import threading
from kafka import KafkaProducer, KafkaConsumer
import sys, os
# producers/ -> go up one level to project root
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from ev_logger import log, log_error
from config.stations import MOVE_STEP, TRAVEL_STEP_KM, ARRIVE_THRESHOLD_KM

# Configuration
NUM_PAIRS        = 40          # 40 heuristic + 40 AI = 80 cars total
UPDATE_INTERVAL  = 1.25
TOPIC_REAL       = "cars_real"
TOPIC_COMMANDS   = "charging_commands"
BOOTSTRAP_SERVERS = os.environ.get('KAFKA_BOOTSTRAP_SERVERS', 'localhost:9092')
BATTERY_CAPACITY_KWH = 60.0
GRID_SIZE        = 10.0        # km — grid spans 0..10 in both axes


class Car:
    def __init__(self, car_id, group, start_battery, priority, k, x, y):
        self.id       = car_id
        self.group    = group          # 'heuristic' | 'ai'
        self.battery_level = start_battery
        self.priority = priority
        self.k        = k              # % discharge per tick
        self.is_charging = False
        self.is_parked   = False
        self.x = x
        self.y = y
        self.target_x = None          # station x to travel toward (None = roam freely)
        self.target_y = None

    def update(self, demand_mult: float = 1.0):
        if self.is_charging:
            log('CAR_UPDATE', f"{self.id} | CHARGING | batt={self.battery_level:.1f}%")
        elif self.target_x is not None:
            # Directed travel toward assigned station
            dx   = self.target_x - self.x
            dy   = self.target_y - self.y
            dist = math.sqrt(dx * dx + dy * dy)
            if dist <= ARRIVE_THRESHOLD_KM:
                # Snap to station and clear target
                self.x       = self.target_x
                self.y       = self.target_y
                self.target_x = None
                self.target_y = None
                log('CAR_UPDATE', f"{self.id} | ARRIVED | pos=({self.x:.1f},{self.y:.1f})")
            else:
                self.x = max(0.0, min(GRID_SIZE, self.x + (dx / dist) * TRAVEL_STEP_KM))
                self.y = max(0.0, min(GRID_SIZE, self.y + (dy / dist) * TRAVEL_STEP_KM))
                log('CAR_UPDATE',
                    f"{self.id} | TRANSIT | batt={self.battery_level:.1f}% | "
                    f"dist={dist:.2f}km | pos=({self.x:.1f},{self.y:.1f})")
            self.battery_level = max(0.0, self.battery_level - self.k * demand_mult)
        elif not self.is_parked:
            self.battery_level = max(0.0, self.battery_level - self.k * demand_mult)
            self.x = max(0.0, min(GRID_SIZE, self.x + random.uniform(-MOVE_STEP, MOVE_STEP)))
            self.y = max(0.0, min(GRID_SIZE, self.y + random.uniform(-MOVE_STEP, MOVE_STEP)))
            log('CAR_UPDATE',
                f"{self.id} | DRIVING | batt={self.battery_level:.1f}% | "
                f"demand_mult={demand_mult:.1f} | pos=({self.x:.1f},{self.y:.1f})")

    def to_dict(self):
        return {
            "id":                  self.id,
            "group":               self.group,
            "current_battery_kwh": (self.battery_level / 100.0) * BATTERY_CAPACITY_KWH,
            "current_soc":         self.battery_level / 100.0,
            "priority":            self.priority,
            "plugged_in":          self.is_parked,
            "x":                   round(self.x, 4),
            "y":                   round(self.y, 4),
            "in_transit":          self.target_x is not None,
            "target_x":            round(self.target_x, 4) if self.target_x is not None else None,
            "target_y":            round(self.target_y, 4) if self.target_y is not None else None,
            "timestamp":           time.time(),
        }


# Global dictionary to access cars from consumer thread
cars_map  = {}
cars_lock = threading.Lock()

# Shared simulated time — updated by the command consumer thread
sim_time      = {'interval_idx': 48, 'day_of_week': 0}
sim_time_lock = threading.Lock()


def demand_multiplier(interval_idx: int, day_of_week: int) -> float:
    """
    Discharge-rate multiplier that mimics real-world EV charging demand.
    Weekdays Mon-Thu 16:00-20:00 = PEAK × 2.5
    Weekdays 00:00-08:00         = NIGHT × 0.3
    Friday night + Saturday      = QUIET × 0.4
    Sunday                       = MODERATE × 0.7
    Everything else              = NORMAL × 1.0
    """
    if (day_of_week == 4 and interval_idx >= 72) or day_of_week == 5:
        return 0.4
    if day_of_week == 6:
        return 0.7
    if day_of_week <= 3:
        if interval_idx < 32:
            return 0.3
        if 64 <= interval_idx < 80:
            return 2.5
    return 1.0


def consume_commands():
    """Background thread to listen for charging commands from Flink."""
    consumer = KafkaConsumer(
        TOPIC_COMMANDS,
        bootstrap_servers=BOOTSTRAP_SERVERS,
        group_id='car_producer_feedback',
        value_deserializer=lambda x: json.loads(x.decode('utf-8'))
    )
    print("Command Consumer Started...")
    for message in consumer:
        cmd    = message.value
        car_id = cmd.get('car_id')
        action = cmd.get('action')
        new_soc = cmd.get('new_soc')

        with sim_time_lock:
            if cmd.get('interval_idx') is not None:
                sim_time['interval_idx'] = cmd['interval_idx']
            if cmd.get('day_of_week') is not None:
                sim_time['day_of_week'] = cmd['day_of_week']

        with cars_lock:
            if car_id in cars_map:
                car = cars_map[car_id]
                if action == 'SET_TARGET':
                    tx = cmd.get('station_x')
                    ty = cmd.get('station_y')
                    if tx is not None and ty is not None:
                        car.target_x = float(tx)
                        car.target_y = float(ty)
                        log('CAR_PROD', f"{car_id} | SET_TARGET | ({tx:.1f},{ty:.1f})")
                elif action == 'START_CHARGING':
                    log('CAR_PROD',
                        f"{car_id} | START_CHARGING | new_soc={new_soc} | batt_before={car.battery_level:.1f}%")
                    if new_soc is not None:
                        car.battery_level = new_soc * 100.0
                    # Snap to station position
                    sx = cmd.get('station_x')
                    sy = cmd.get('station_y')
                    if sx is not None and sy is not None:
                        car.x = float(sx)
                        car.y = float(sy)
                    car.target_x    = None
                    car.target_y    = None
                    car.is_charging = True
                    car.is_parked   = True
                elif action == 'STOP_CHARGING':
                    log('CAR_PROD',
                        f"{car_id} | STOP_CHARGING | new_soc={new_soc} | batt_before={car.battery_level:.1f}%")
                    if new_soc is not None:
                        car.battery_level = new_soc * 100.0
                    car.is_charging = False
                    car.is_parked   = False


def main():
    print("Starting Car Producer — 40 heuristic + 40 AI pairs (identical starting conditions)…")

    producer = KafkaProducer(
        bootstrap_servers=BOOTSTRAP_SERVERS,
        value_serializer=lambda v: json.dumps(v).encode('utf-8')
    )

    # Create 40 pairs: h_car_i and a_car_i start with the same battery%, k, and position
    for i in range(1, NUM_PAIRS + 1):
        start_battery = random.uniform(10.0, 100.0)
        priority      = random.randint(1, 5)
        discharge_k   = random.uniform(0.4, 1.0)   # % per tick
        x             = random.uniform(0.0, GRID_SIZE)
        y             = random.uniform(0.0, GRID_SIZE)

        cars_map[f"h_car_{i}"] = Car(f"h_car_{i}", "heuristic", start_battery, priority, discharge_k, x, y)
        cars_map[f"a_car_{i}"] = Car(f"a_car_{i}", "ai",        start_battery, priority, discharge_k, x, y)

    print(f"Initialized {len(cars_map)} cars ({NUM_PAIRS} heuristic + {NUM_PAIRS} AI).")

    t = threading.Thread(target=consume_commands, daemon=True)
    t.start()

    try:
        while True:
            with sim_time_lock:
                s_idx = sim_time['interval_idx']
                s_dow = sim_time['day_of_week']
            mult = demand_multiplier(s_idx, s_dow)

            with cars_lock:
                items = list(cars_map.items())
            for car_id, car in items:
                with cars_lock:
                    car.update(demand_mult=mult)
                    data = car.to_dict()
                producer.send(TOPIC_REAL, data)

            producer.flush()
            time.sleep(UPDATE_INTERVAL)

    except KeyboardInterrupt:
        print("\nStopping car producer…")
    finally:
        producer.close()


if __name__ == "__main__":
    main()
