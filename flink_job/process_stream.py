"""
EV Fleet Bidding Orchestrator (Flink)
======================================
Replaces the day-ahead price / overnight scheduling pipeline with a
real-time bidding loop:

  • On each 1.25 s tick the Flink timer fires for every car and calls
    decide_charge(car, current_price).
  • current_price is updated whenever a DYNAMIC_PRICE message arrives
    from the energy producer (produced every tick, driven by occupancy).
  • active_chargers_count is maintained in Flink state and used by the
    producer to compute the next price (feedback loop).
  • Emergency override (SOC < 15%) always grants a charger slot,
    bypassing the price check — identical safety behaviour to before.
  • Test-car (naive) logic is preserved unchanged for baseline comparison.
"""

import json
import logging
import sys
import os
import time

# Add parent directory to path so we can import optimizers
project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.append(os.path.join(project_root, "optimizers"))

from pyflink.common import Types, SimpleStringSchema
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.datastream.connectors.kafka import (
    KafkaSource, KafkaSink, KafkaRecordSerializationSchema, DeliveryGuarantee,
)
from pyflink.datastream.functions import KeyedProcessFunction, RuntimeContext
from pyflink.datastream.state import ValueStateDescriptor, MapStateDescriptor
from pyflink.common import WatermarkStrategy

# Import the bidding decision module
from heuristic_scheduler import decide_charge, STATION_CAPACITY, BASE_PRICE, MAX_POWER_PER_CHARGER

import sys, os
try:
    from ev_logger import log, log_command, log_charging_decision, log_error
except ModuleNotFoundError:
    import logging as _logging
    _logging.basicConfig(level=_logging.INFO, format='%(asctime)s [%(name)s] %(message)s')
    def log(comp, msg):            _logging.getLogger(comp).info(msg)
    def log_command(*a):           _logging.getLogger('CMD').info(str(a))
    def log_charging_decision(*a): _logging.getLogger('CHARGE').info(str(a))
    def log_error(c, e):           _logging.getLogger(c).error(str(e))

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------
BOOTSTRAP_SERVERS = 'kafka:29092'
GROUP_ID          = 'flink_ev_processor'
TICK_MS           = 1250   # simulation tick in milliseconds


class EVFleetProcessor(KeyedProcessFunction):

    def open(self, runtime_context: RuntimeContext):
        # --- Car state ---
        self.car_state = runtime_context.get_map_state(
            MapStateDescriptor("cars", Types.STRING(), Types.STRING()))

        # --- Bidding state ---
        # Current dynamic price received from the energy producer
        self.current_price_state = runtime_context.get_state(
            ValueStateDescriptor("current_price", Types.FLOAT()))
        # Count of real cars currently charging (used only for logging / sanity)
        self.active_chargers_state = runtime_context.get_state(
            ValueStateDescriptor("active_chargers", Types.INT()))

        # --- Heartbeat ---
        self.timer_set_state = runtime_context.get_state(
            ValueStateDescriptor("timer_set", Types.BOOLEAN()))
        self.interval_state = runtime_context.get_state(
            ValueStateDescriptor("interval_idx", Types.INT()))

        # --- Day-of-week tracking (0=Mon … 6=Sun, starts on Monday) ---
        self.day_of_week_state = runtime_context.get_state(
            ValueStateDescriptor("day_of_week", Types.INT()))

        # --- Per-car charging tracking ---
        self.was_charging = runtime_context.get_map_state(
            MapStateDescriptor("was_charging", Types.STRING(), Types.BOOLEAN()))

        # --- Test-car naive-charging state ---
        self.test_charging_state = runtime_context.get_map_state(
            MapStateDescriptor("test_charging", Types.STRING(), Types.BOOLEAN()))

    # -----------------------------------------------------------------------
    def process_element(self, value, ctx: KeyedProcessFunction.Context):
        event_type, data = value

        if event_type == 'PRICE':
            # Dynamic price update from the energy producer
            price = data.get('current_price')
            if price is not None:
                self.current_price_state.update(float(price))
                active = data.get('active_chargers', 0)
                log('PRICE', f'Dynamic price={price:.2f} EUR/MWh | active_chargers={active}')

        elif event_type == 'CAR':
            car_id = data.get('id')
            if not car_id:
                return
            existing_str = self.car_state.get(car_id)

            if existing_str:
                existing = json.loads(existing_str)
                flink_owns = existing.get('flink_managed', False)
                if flink_owns:
                    # Flink owns SOC — only update non-battery fields from sensor
                    existing['priority']  = data.get('priority', existing['priority'])
                    existing['plugged_in'] = data.get('plugged_in', False)
                    self.car_state.put(car_id, json.dumps(existing))
                else:
                    data['flink_managed'] = False
                    self.car_state.put(car_id, json.dumps(data))
            else:
                data['flink_managed'] = False
                self.car_state.put(car_id, json.dumps(data))

        # Arm the simulation heartbeat on first event
        if not self.timer_set_state.value():
            now = ctx.timer_service().current_processing_time()
            ctx.timer_service().register_processing_time_timer(now + TICK_MS)
            self.timer_set_state.update(True)

    # -----------------------------------------------------------------------
    def on_timer(self, timestamp, ctx: KeyedProcessFunction.OnTimerContext):
        idx = self.interval_state.value()
        if idx is None:
            idx = 0

        # Midnight roll-over: advance day_of_week (0=Mon … 6=Sun)
        if idx >= 96:
            dow = (self.day_of_week_state.value() or 0) + 1
            self.day_of_week_state.update(dow % 7)
            log('SYSTEM', f'MIDNIGHT — day_of_week now {dow % 7}')
            idx = 0

        self.interval_state.update(idx + 1)
        day_of_week = self.day_of_week_state.value() or 0

        # Current dynamic price (fall back to base if no message received yet)
        current_price = self.current_price_state.value()
        if current_price is None:
            current_price = BASE_PRICE

        car_ids = list(self.car_state.keys())

        # Extract cars and shuffle/sort by SOC to prevent alphabetical map-key bias
        # meaning lowest battery cars naturally get evaluated first for slots
        car_list = []
        for cid in car_ids:
            cdr = self.car_state.get(cid)
            if cdr:
                car_list.append((cid, json.loads(cdr)))
                
        import random
        random.shuffle(car_list)
        car_list.sort(key=lambda x: x[1].get('current_soc', 1.0))

        # Re-calculate active chargers from scratch based on strict priority allocation
        total_busy = 0
        new_test_active = 0
        new_real_active = 0

        for car_id, car_data in car_list:
            current_soc = car_data.get('current_soc', 1.0)
            target_power = 0.0
            reason_str = ""

            if car_id.endswith('_test'):
                # --- NAIVE TEST-CAR LOGIC ---
                test_active = self.test_charging_state.get(car_id) or False
                wants_charge = False

                if current_soc <= 0.02:
                    wants_charge = True
                elif current_soc >= 0.99:
                    wants_charge = False
                else:
                    wants_charge = test_active

                should_charge = wants_charge and (total_busy < STATION_CAPACITY)

                if should_charge:
                    total_busy += 1
                    new_test_active += 1
                    target_power = MAX_POWER_PER_CHARGER
                    
                self.test_charging_state.put(car_id, should_charge)
                reason_str = "charging" if should_charge else ("station full" if wants_charge else "idle")

            else:
                # --- REAL CAR: bidding decision ---
                prev_charging = self.was_charging.get(car_id) or False
                emergency     = current_soc < 0.15
                recovering    = car_data.get('emergency_charging', False) and current_soc < 0.40

                # Determine if car *would* charge if there was space
                car_wants_to_charge = decide_charge(car_data, current_price, prev_charging)
                if emergency or recovering:
                    car_wants_to_charge = True

                should_charge = car_wants_to_charge and (total_busy < STATION_CAPACITY)

                if should_charge:
                    total_busy += 1
                    new_real_active += 1
                    target_power = MAX_POWER_PER_CHARGER

                    if emergency or recovering:
                        car_data['emergency_charging'] = True
                        reason_str = "emergency"
                        log('FLINK', f'{car_id} | EMERGENCY | soc={current_soc*100:.1f}% | price={current_price:.2f}')
                    else:
                        car_data['emergency_charging'] = False
                        reason_str = "charging"
                        log('FLINK', f'{car_id} | CHARGING | soc={current_soc*100:.1f}% | price={current_price:.2f}')
                else:
                    car_data['emergency_charging'] = False
                    if car_wants_to_charge:
                        reason_str = "station full"
                        pre = "EMERGENCY " if (emergency or recovering) else ""
                        log('FLINK', f'{car_id} | {pre}DEFERRED (station full) | soc={current_soc*100:.1f}% | price={current_price:.2f}')
                    else:
                        reason_str = "price too high"
                        log('FLINK', f'{car_id} | DEFERRED (price) | soc={current_soc*100:.1f}% | price={current_price:.2f}')

            self.was_charging.put(car_id, should_charge)

            # Build command
            command = {
                'car_id':         car_id,
                'interval_idx':   idx,
                'day_of_week':    day_of_week,   # 0=Mon … 6=Sun
                'action':         'START_CHARGING' if should_charge else 'STOP_CHARGING',
                'reason':         reason_str,
                'power_kw':       target_power,
                'timestamp':      time.time(),
                'charging_price': round(current_price, 4) if should_charge else None,
            }

            if should_charge:
                energy_added         = target_power * 0.25   # 15-sim-min = 0.25h
                new_kwh              = min(60.0, car_data.get('current_battery_kwh', 60.0) + energy_added)
                new_kwh              = max(new_kwh, car_data.get('current_battery_kwh', 60.0))
                car_data['current_battery_kwh'] = new_kwh
                car_data['current_soc']         = new_kwh / 60.0
                car_data['plugged_in']           = True
                car_data['flink_managed']        = True
                self.car_state.put(car_id, json.dumps(car_data))
                command['new_soc'] = car_data['current_soc']
            else:
                if car_data.get('plugged_in', False):
                    car_data['plugged_in']    = False
                    car_data['flink_managed'] = False
                    self.car_state.put(car_id, json.dumps(car_data))
                real_soc = car_data.get('current_soc')
                command['new_soc'] = real_soc if (real_soc is not None and real_soc > 0.01) else None

            log_command(car_id, command['action'], command.get('new_soc'), idx)
            yield json.dumps(command)

        # Persist updated real charger count
        self.active_chargers_state.update(new_real_active)

        # Schedule next tick relative to fired timestamp to prevent drift
        ctx.timer_service().register_processing_time_timer(timestamp + TICK_MS)


# ---------------------------------------------------------------------------
def main():
    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_parallelism(1)
    env.enable_checkpointing(10000)

    project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    env.add_python_file(os.path.join(project_root, "optimizers", "heuristic_scheduler.py"))
    ev_logger_path = os.path.join(project_root, "ev_logger.py")
    if os.path.exists(ev_logger_path):
        env.add_python_file(ev_logger_path)

    jar_path = os.path.join(project_root, "flink-sql-connector-kafka-1.17.0.jar")
    if os.path.exists(jar_path):
        env.add_jars("file://" + jar_path)

    energy_source = (
        KafkaSource.builder()
        .set_bootstrap_servers(BOOTSTRAP_SERVERS)
        .set_topics("energy_data")
        .set_group_id(GROUP_ID)
        .set_value_only_deserializer(SimpleStringSchema())
        .build()
    )
    car_source = (
        KafkaSource.builder()
        .set_bootstrap_servers(BOOTSTRAP_SERVERS)
        .set_topics("cars_real")
        .set_group_id(GROUP_ID)
        .set_value_only_deserializer(SimpleStringSchema())
        .build()
    )

    ds_energy  = env.from_source(energy_source, WatermarkStrategy.no_watermarks(), "Energy").map(
        lambda x: ('PRICE', json.loads(x)))
    ds_cars    = env.from_source(car_source, WatermarkStrategy.no_watermarks(), "Cars").map(
        lambda x: ('CAR', json.loads(x)))

    ds_keyed   = ds_energy.union(ds_cars).key_by(lambda x: "global")
    ds_commands = ds_keyed.process(EVFleetProcessor(), output_type=Types.STRING())

    command_sink = (
        KafkaSink.builder()
        .set_bootstrap_servers(BOOTSTRAP_SERVERS)
        .set_record_serializer(
            KafkaRecordSerializationSchema.builder()
            .set_topic("charging_commands")
            .set_value_serialization_schema(SimpleStringSchema())
            .build()
        )
        .set_delivery_guarantee(DeliveryGuarantee.AT_LEAST_ONCE)
        .build()
    )
    ds_commands.sink_to(command_sink)
    env.execute("EV Fleet Bidding Orchestrator")


if __name__ == '__main__':
    main()