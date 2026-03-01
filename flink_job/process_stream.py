import json
import logging
import sys
import os
import time

# Add parent directory to path so we can import optimizers
project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.append(os.path.join(project_root, "optimizers"))

from pyflink.common import Types, SimpleStringSchema, Time
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.datastream.connectors.kafka import KafkaSource, KafkaSink, KafkaRecordSerializationSchema, DeliveryGuarantee
from pyflink.datastream.functions import KeyedProcessFunction, RuntimeContext
from pyflink.datastream.state import ValueStateDescriptor, MapStateDescriptor

# Import the Heuristic Scheduler
from heuristic_scheduler import smart_heuristic_schedule

# Configuration
BOOTSTRAP_SERVERS = 'kafka:29092' 
GROUP_ID = 'flink_ev_processor'

class EVFleetProcessor(KeyedProcessFunction):
    def open(self, runtime_context: RuntimeContext):
        # We store Today and Tomorrow separately for clear shifting
        self.today_prices = runtime_context.get_state(ValueStateDescriptor("today_prices", Types.STRING()))
        self.tomorrow_prices = runtime_context.get_state(ValueStateDescriptor("tomorrow_prices", Types.STRING()))
        
        self.car_state = runtime_context.get_map_state(MapStateDescriptor("cars", Types.STRING(), Types.STRING()))
        self.interval_state = runtime_context.get_state(ValueStateDescriptor("interval_idx", Types.INT()))
        self.timer_set_state = runtime_context.get_state(ValueStateDescriptor("timer_set", Types.BOOLEAN()))

    def process_element(self, value, ctx: KeyedProcessFunction.Context):
        event_type, data = value
        
        if event_type == 'PRICE':
            # Market cleared at 13:00! These are prices for TOMORROW.
            current_tmrw_str = self.tomorrow_prices.value()
            current_tmrw = json.loads(current_tmrw_str) if current_tmrw_str else []
            current_tmrw.append(data.get('AT_price_day_ahead', 0.0))
            
            # If we received a full new day (96 rows), save it
            if len(current_tmrw) >= 96:
                self.tomorrow_prices.update(json.dumps(current_tmrw[:96]))
            else:
                self.tomorrow_prices.update(json.dumps(current_tmrw))

        elif event_type == 'CAR':
            # Race condition fix: 
            # If Flink is currently charging this car, ignore the incoming telemetry
            # because the producer might be sending old battery data that hasn't 
            # been updated by our commands yet.
            car_id = data['id']
            existing_car_str = self.car_state.get(car_id)
            if existing_car_str:
                existing_car = json.loads(existing_car_str)
                if existing_car.get('plugged_in', False):
                    # Car is charging! Keep our calculated battery, only update other fields if needed.
                    existing_car['priority'] = data.get('priority', existing_car['priority'])
                    self.car_state.put(car_id, json.dumps(existing_car))
                else:
                    # Car is driving, sensor data is the truth.
                    self.car_state.put(car_id, json.dumps(data))
            else:
                self.car_state.put(car_id, json.dumps(data))

        # Start simulation heartbeat
        if not self.timer_set_state.value():
            current_time = ctx.timer_service().current_processing_time()
            ctx.timer_service().register_processing_time_timer(current_time + 1250)
            self.timer_set_state.update(True)

    def on_timer(self, timestamp, ctx: KeyedProcessFunction.OnTimerContext):
        idx = self.interval_state.value()
        if idx is None: idx = 0
        
        # --- MIDNIGHT SHIFT ---
        if idx >= 96:
            idx = 0
            # Tomorrow becomes Today
            tmrw_str = self.tomorrow_prices.value()
            self.today_prices.update(tmrw_str if tmrw_str else "[]")
            self.tomorrow_prices.clear() # Wait for next 13:00 clearing

        self.interval_state.update(idx)
        
        # Build the full 48h price window for the scheduler
        today_list = json.loads(self.today_prices.value()) if self.today_prices.value() else []
        tmrw_list = json.loads(self.tomorrow_prices.value()) if self.tomorrow_prices.value() else []
        
        # Padding Today if empty (at simulation start)
        if not today_list:
            today_list = [999.0] * 96 # Price is infinite so no charging Today
            
        full_prices = today_list + tmrw_list
        
        cars = []
        car_ids = list(self.car_state.keys())
        for car_id in car_ids:
            cars.append(json.loads(self.car_state.get(car_id)))

        # Run Optimizer
        schedule = {}
        if len(full_prices) >= 96:
            # We always pass the full window. 
            # If we are at sim start, it only finds slots in tmrw_list (starts at index 96).
            schedule = smart_heuristic_schedule(full_prices, cars, idx)
        
        # Broadcast commands & battery simulation
        for car_id in car_ids:
            plan = schedule.get(car_id, [])
            
            should_charge = False
            target_power = 0.0
            for slot in plan:
                if slot['interval'] == idx:
                    should_charge = True
                    target_power = slot['power_kw']
                    break
            
            command = {
                "car_id": car_id,
                "interval_idx": idx,
                "action": "START_CHARGING" if should_charge else "STOP_CHARGING",
                "power_kw": target_power,
                "timestamp": time.time(),
                "plan": plan
            }
            
            if should_charge:
                car_data = json.loads(self.car_state.get(car_id))
                energy_added = target_power * 0.25
                new_kwh = min(60.0, car_data['current_battery_kwh'] + energy_added)
                car_data['current_battery_kwh'] = new_kwh
                car_data['current_soc'] = new_kwh / 60.0
                self.car_state.put(car_id, json.dumps(car_data))
                command['new_soc'] = car_data['current_soc']
            else:
                car_data = json.loads(self.car_state.get(car_id))
                command['new_soc'] = car_data.get('current_soc', 0.0)

            yield json.dumps(command)

        # Heartbeat
        self.interval_state.update(idx + 1)
        ctx.timer_service().register_processing_time_timer(ctx.timer_service().current_processing_time() + 1250)

def main():
    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_parallelism(1)
    
    project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    env.add_python_file(os.path.join(project_root, "optimizers", "heuristic_scheduler.py"))

    jar_path = os.path.join(project_root, "flink-sql-connector-kafka-1.17.0.jar")
    if os.path.exists(jar_path):
        env.add_jars("file://" + jar_path)

    energy_source = KafkaSource.builder().set_bootstrap_servers('kafka:29092').set_topics("energy_data").set_group_id(GROUP_ID).set_value_only_deserializer(SimpleStringSchema()).build()
    car_source = KafkaSource.builder().set_bootstrap_servers('kafka:29092').set_topics("cars_real").set_group_id(GROUP_ID).set_value_only_deserializer(SimpleStringSchema()).build()
    
    ds_energy = env.from_source(energy_source, WatermarkStrategy.no_watermarks(), "Energy").map(lambda x: ('PRICE', json.loads(x)))
    ds_cars = env.from_source(car_source, WatermarkStrategy.no_watermarks(), "Cars").map(lambda x: ('CAR', json.loads(x)))

    ds_keyed = ds_energy.union(ds_cars).key_by(lambda x: "global")
    ds_commands = ds_keyed.process(EVFleetProcessor(), output_type=Types.STRING())

    command_sink = KafkaSink.builder().set_bootstrap_servers('kafka:29092').set_record_serializer(KafkaRecordSerializationSchema.builder().set_topic("charging_commands").set_value_serialization_schema(SimpleStringSchema()).build()).set_delivery_guarantee(DeliveryGuarantee.AT_LEAST_ONCE).build()
    ds_commands.sink_to(command_sink)
    env.execute("EV Fleet Cost Optimizer")

if __name__ == '__main__':
    from pyflink.common import WatermarkStrategy
    main()