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
from pyflink.datastream.connectors.kafka import KafkaSource, KafkaSink, KafkaRecordSerializationSchema, \
    DeliveryGuarantee
from pyflink.datastream.functions import KeyedProcessFunction, RuntimeContext
from pyflink.datastream.state import ValueStateDescriptor, MapStateDescriptor
from pyflink.common import WatermarkStrategy

# Import the Heuristic Scheduler
from heuristic_scheduler import smart_heuristic_schedule
import sys, os
# Try to import ev_logger — works locally and when registered via add_python_file()
try:
    from ev_logger import log, log_price, log_schedule, log_command, log_charging_decision, log_error
except ModuleNotFoundError:
    # Fallback: write to stderr so logs still appear in Flink TaskManager logs
    import logging as _logging
    _logging.basicConfig(level=_logging.INFO, format='%(asctime)s [%(name)s] %(message)s')
    def log(comp, msg):           _logging.getLogger(comp).info(msg)
    def log_price(*a):            _logging.getLogger('PRICE').info(str(a))
    def log_schedule(*a, **kw):   _logging.getLogger('SCHED').info(str(a) + str(kw))
    def log_command(*a):          _logging.getLogger('CMD').info(str(a))
    def log_charging_decision(*a):_logging.getLogger('CHARGE').info(str(a))
    def log_error(c, e):          _logging.getLogger(c).error(str(e))

# Configuration
BOOTSTRAP_SERVERS = 'kafka:29092'
GROUP_ID = 'flink_ev_processor'


class EVFleetProcessor(KeyedProcessFunction):
    def open(self, runtime_context: RuntimeContext):
        # We store Today and Tomorrow separately for clear shifting
        self.today_prices = runtime_context.get_state(ValueStateDescriptor("today_prices", Types.STRING()))
        self.tomorrow_prices = runtime_context.get_state(ValueStateDescriptor("tomorrow_prices", Types.STRING()))

        self.car_state = runtime_context.get_map_state(MapStateDescriptor("cars", Types.STRING(), Types.STRING()))
        self.plan_state = runtime_context.get_map_state(MapStateDescriptor("plans", Types.STRING(), Types.STRING()))
        self.soc_at_plan = runtime_context.get_map_state(MapStateDescriptor("soc_at_plan", Types.STRING(), Types.FLOAT()))
        self.interval_state = runtime_context.get_state(ValueStateDescriptor("interval_idx", Types.INT()))
        self.timer_set_state = runtime_context.get_state(ValueStateDescriptor("timer_set", Types.BOOLEAN()))
        self.charging_enabled_state = runtime_context.get_state(ValueStateDescriptor("charging_enabled", Types.BOOLEAN()))
        self.tomorrow_prices_ready_state = runtime_context.get_state(ValueStateDescriptor("tomorrow_prices_ready", Types.BOOLEAN()))

    def process_element(self, value, ctx: KeyedProcessFunction.Context):
        event_type, data = value

        if event_type == 'PRICE':
            price     = data.get('AT_price_day_ahead', 0.0)
            msg_type  = data.get('type', 'TOMORROW')  # 'TODAY' or 'TOMORROW'

            if msg_type == 'TODAY':
                current_today_str = self.today_prices.value()
                current_today = json.loads(current_today_str) if current_today_str else []
                current_today.append(price)
                if len(current_today) >= 96:
                    self.today_prices.update(json.dumps(current_today[:96]))
                else:
                    self.today_prices.update(json.dumps(current_today))
                log_price('TODAY', price, len(current_today), len(json.loads(self.tomorrow_prices.value()) if self.tomorrow_prices.value() else []))
            else:
                current_tmrw_str = self.tomorrow_prices.value()
                current_tmrw = json.loads(current_tmrw_str) if current_tmrw_str else []
                current_tmrw.append(price)
                if len(current_tmrw) >= 96:
                    self.tomorrow_prices.update(json.dumps(current_tmrw[:96]))
                    self.tomorrow_prices_ready_state.update(True)  # Full batch received — ready to plan at 23:00
                else:
                    self.tomorrow_prices.update(json.dumps(current_tmrw))
                log_price('TOMORROW', price, len(json.loads(self.today_prices.value()) if self.today_prices.value() else []), len(current_tmrw))

        elif event_type == 'CAR':
            car_id = data.get('id')
            if not car_id:
                return
            existing_car_str = self.car_state.get(car_id)
            is_physically_plugged = data.get('plugged_in', False)

            if existing_car_str:
                existing_car = json.loads(existing_car_str)
                flink_owns_state = existing_car.get('flink_managed', False)

                if flink_owns_state:
                    # Flink owns the battery state — NEVER overwrite SOC with sensor data,
                    # even if plugged_in=False (stale sensor arrives before car processes START).
                    # Only accept non-battery fields so we don't lose our calculated SOC.
                    existing_car['priority'] = data.get('priority', existing_car['priority'])
                    existing_car['plugged_in'] = is_physically_plugged  # track physical state
                    self.car_state.put(car_id, json.dumps(existing_car))
                else:
                    # Flink released control — sensor is ground truth.
                    data['flink_managed'] = False
                    self.car_state.put(car_id, json.dumps(data))
            else:
                data['flink_managed'] = False
                self.car_state.put(car_id, json.dumps(data))

        # Start simulation heartbeat — 2500ms per interval (15 sim-min / 6 sim-min per real-sec)
        if not self.timer_set_state.value():
            current_time = ctx.timer_service().current_processing_time()
            ctx.timer_service().register_processing_time_timer(current_time + 1250)
            self.timer_set_state.update(True)

    def on_timer(self, timestamp, ctx: KeyedProcessFunction.OnTimerContext):
        idx = self.interval_state.value()
        if idx is None: idx = 48  # Start at 12:00 (interval 48 = 48 × 15 min)

        # --- MIDNIGHT SHIFT ---
        if idx >= 96:
            idx = 0
            # Tomorrow becomes Today
            tmrw_str = self.tomorrow_prices.value()
            self.today_prices.update(tmrw_str if tmrw_str else "[]")
            self.tomorrow_prices.clear()  # Wait for next 13:00 clearing
            self.charging_enabled_state.update(True)   # Enable charging — full day ahead now available
            self.tomorrow_prices_ready_state.update(False)  # Reset — wait for next 13:00 batch
            log('SYSTEM', 'MIDNIGHT SHIFT — charging enabled, tomorrow is now today')

        # Build the full 48h price window for the scheduler
        today_list = json.loads(self.today_prices.value()) if self.today_prices.value() else []
        tmrw_list = json.loads(self.tomorrow_prices.value()) if self.tomorrow_prices.value() else []

        # Only pad if truly empty AND tomorrow also has no data yet
        # (very first seconds of startup before any price message arrives)
        if not today_list and not tmrw_list:
            log('SYSTEM', 'WARNING: No price data available — blocking charging with penalty prices')
            today_list = [999.0] * 96  # No data yet — block charging temporarily
        elif not today_list and tmrw_list:
            # We have tomorrow but not today — use tomorrow as today (producer sent TOMORROW first)
            log('SYSTEM', 'WARNING: Today prices missing — using tomorrow as today')
            today_list = tmrw_list
            tmrw_list = []

        full_prices = today_list + tmrw_list

        car_ids = list(self.car_state.keys())
        cars = [json.loads(self.car_state.get(c)) for c in car_ids]

        # --- STABLE SCHEDULE LOGIC ---
        # Only re-plan when necessary. Re-planning every tick breaks session limits
        # because sessions_count resets each run, and the "cheapest slot" is never
        # the current interval — so charging looks random and ignores the schedule.
        #
        # Re-plan a car if:
        #   (a) it has no stored plan yet, OR
        #   (b) it's a new day (idx == 0), OR
        #   (c) its SOC dropped >10% below what it was when we last planned
        #       (it drove more than expected — need a new plan)
        cars_needing_replan = []
        for car in cars:
            car_id = car.get('id')
            if not car_id:
                continue
            # Use get() not contains() — PyFlink MapState.contains() can return
            # False even when key exists, causing infinite replanning.
            stored_plan_str = self.plan_state.get(car_id)
            has_plan = stored_plan_str is not None
            soc_now  = car.get('current_soc', 0.0)

            soc_then = None
            if has_plan:
                try:
                    soc_then = self.soc_at_plan.get(car_id)
                except Exception:
                    soc_then = None

            # Check if ALL plan slots are in the past — root cause of no charging
            all_slots_expired = False
            if has_plan:
                try:
                    _stored = json.loads(stored_plan_str)
                    if _stored:
                        all_slots_expired = max(s['interval'] for s in _stored) < idx
                    else:
                        all_slots_expired = True
                except Exception:
                    all_slots_expired = True

            needs_replan = (
                not has_plan
                or all_slots_expired
                or idx == 0
                or (soc_then is not None and soc_now < soc_then - 0.10)
            )
            reason = ('no_plan' if not has_plan
                      else 'expired' if all_slots_expired
                      else 'new_day' if idx == 0
                      else 'soc_drop')
            log(f'FLINK', f'{car_id} | has_plan={has_plan} | soc_now={soc_now:.2f} | soc_then={soc_then} | needs_replan={needs_replan} | reason={reason}')
            if needs_replan:
                cars_needing_replan.append(car)

        charging_enabled = self.charging_enabled_state.value() or False
        tomorrow_prices_ready = self.tomorrow_prices_ready_state.value() or False

        # On subsequent days: force a full replan at 23:00 (idx=92) when new tomorrow prices are ready
        # On first day: replan happens naturally (charging_enabled=False, plans from idx=0)
        if charging_enabled and tomorrow_prices_ready and idx >= 92:
            log('SYSTEM', f'23:00 PLANNING TRIGGER — replanning all cars for tomorrow')
            cars_needing_replan = [car for car in cars if car.get('id')]
            self.tomorrow_prices_ready_state.update(False)  # Consume the flag

        if cars_needing_replan and full_prices:
            # Pre-midnight: plan from idx=0 so schedule covers the full day starting at midnight
            schedule_idx = idx if charging_enabled else 0
            new_schedules = smart_heuristic_schedule(full_prices, cars_needing_replan, schedule_idx)
            for car in cars_needing_replan:
                car_id = car['id']
                plan   = new_schedules.get(car_id, [])
                self.plan_state.put(car_id, json.dumps(plan))
                self.soc_at_plan.put(car_id, car.get('current_soc', 0.0))
                intervals = [s['interval'] for s in plan]
                log_schedule(car_id, intervals, idx, reason='replan')

        # Broadcast commands using the STORED stable plan, not a freshly generated one
        for car_id in car_ids:
            # Guard: skip if car state was never written (race between plan and first CAR event)
            car_data_str = self.car_state.get(car_id)
            if car_data_str is None:
                continue
            car_data = json.loads(car_data_str)

            stored_plan_str = self.plan_state.get(car_id)
            plan = json.loads(stored_plan_str) if stored_plan_str else []

            should_charge = False
            target_power  = 0.0
            plan_intervals = [s['interval'] for s in plan]

            # --- EMERGENCY CHARGING ---
            # If SOC drops below 15%, override the schedule and charge immediately until 40%
            current_soc = car_data.get('current_soc', 1.0)
            emergency = current_soc < 0.15
            recovering = car_data.get('emergency_charging', False) and current_soc < 0.40

            if emergency or recovering:
                # Emergency bypasses the charging_enabled gate — safety always wins
                should_charge = True
                target_power  = 11.0  # Max charge rate during emergency
                car_data['emergency_charging'] = True
                log('FLINK', f'{car_id} | EMERGENCY CHARGING | soc={current_soc*100:.1f}%')
            elif charging_enabled:
                car_data['emergency_charging'] = False
                for slot in plan:
                    if slot['interval'] == idx:
                        should_charge = True
                        target_power  = slot['power_kw']
                        break
            else:
                # Pre-midnight — no charging, only planning
                car_data['emergency_charging'] = False
                log('FLINK', f'{car_id} | PRE-MIDNIGHT — charging suppressed | soc={current_soc*100:.1f}%')

            log_charging_decision(car_id, idx, plan_intervals, should_charge)

            command = {
                "car_id": car_id,
                "interval_idx": idx,
                "action": "START_CHARGING" if should_charge else "STOP_CHARGING",
                "power_kw": target_power,
                "timestamp": time.time(),
                "plan": plan
            }

            if should_charge:
                energy_added = target_power * 0.25
                new_kwh = min(60.0, car_data['current_battery_kwh'] + energy_added)
                # Never send a SOC lower than what we already stored — sensor updates can arrive out of order
                new_kwh = max(new_kwh, car_data['current_battery_kwh'])
                car_data['current_battery_kwh'] = new_kwh
                car_data['current_soc'] = new_kwh / 60.0
                car_data['plugged_in'] = True
                car_data['flink_managed'] = True  # Lock: prevent sensor from overwriting our SOC

                self.car_state.put(car_id, json.dumps(car_data))
                command['new_soc'] = car_data['current_soc']
            else:
                if car_data.get('plugged_in', False):
                    car_data['plugged_in'] = False
                    car_data['flink_managed'] = False  # Release: sensor is ground truth again
                    self.car_state.put(car_id, json.dumps(car_data))

                # Only send a real non-zero SOC — default 0.0 would zero the battery
                real_soc = car_data.get('current_soc')
                command['new_soc'] = real_soc if (real_soc is not None and real_soc > 0.01) else None

            log_command(car_id, command['action'], command.get('new_soc'), idx)
            yield json.dumps(command)

        # Heartbeat — use fired timestamp as base to prevent drift and duplicate fires
        self.interval_state.update(idx + 1)
        ctx.timer_service().register_processing_time_timer(timestamp + 1250)


def main():
    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_parallelism(1)

    project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    env.add_python_file(os.path.join(project_root, "optimizers", "heuristic_scheduler.py"))
    ev_logger_path = os.path.join(project_root, "ev_logger.py")
    if os.path.exists(ev_logger_path):
        env.add_python_file(ev_logger_path)

    jar_path = os.path.join(project_root, "flink-sql-connector-kafka-1.17.0.jar")
    if os.path.exists(jar_path):
        env.add_jars("file://" + jar_path)

    energy_source = KafkaSource.builder().set_bootstrap_servers('kafka:29092').set_topics("energy_data").set_group_id(
        GROUP_ID).set_value_only_deserializer(SimpleStringSchema()).build()
    car_source = KafkaSource.builder().set_bootstrap_servers('kafka:29092').set_topics("cars_real").set_group_id(
        GROUP_ID).set_value_only_deserializer(SimpleStringSchema()).build()

    ds_energy = env.from_source(energy_source, WatermarkStrategy.no_watermarks(), "Energy").map(
        lambda x: ('PRICE', json.loads(x)))
    ds_cars = env.from_source(car_source, WatermarkStrategy.no_watermarks(), "Cars").map(
        lambda x: ('CAR', json.loads(x)))

    ds_keyed = ds_energy.union(ds_cars).key_by(lambda x: "global")
    ds_commands = ds_keyed.process(EVFleetProcessor(), output_type=Types.STRING())

    command_sink = KafkaSink.builder().set_bootstrap_servers('kafka:29092').set_record_serializer(
        KafkaRecordSerializationSchema.builder().set_topic("charging_commands").set_value_serialization_schema(
            SimpleStringSchema()).build()).set_delivery_guarantee(DeliveryGuarantee.AT_LEAST_ONCE).build()
    ds_commands.sink_to(command_sink)
    env.execute("EV Fleet Cost Optimizer")


if __name__ == '__main__':
    main()