"""
EV Fleet Bidding Orchestrator (Flink) — with Reservation System
================================================================
Two-phase per-tick processing:

  Phase 1 — Planning: each real car reserves the cheapest future slots.
            Cars are processed lowest-SOC-first so urgent cars get first pick.
            Reservation board tracks per-slot counts so subsequent cars see
            updated projected prices.

  Phase 2 — Execution: allocate chargers by priority:
            1. Emergency (SOC < 15%) — always charge
            2. Reserved  — cars with a reservation for this slot
            3. Opportunistic — heuristic fallback (decide_charge)
            4. Test cars — unchanged naive logic

  After execution, a RESERVATION_STATUS message is emitted on the same
  charging_commands sink so the energy producer and dashboard can show
  projected prices.
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

# Import reservation scheduler
from reservation_scheduler import (
    should_replan, select_cheapest_slots, slots_needed, projected_price,
    SLOTS_PER_DAY,
)

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
        self.current_price_state = runtime_context.get_state(
            ValueStateDescriptor("current_price", Types.FLOAT()))
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

        # --- Reservation state ---
        # key: str(slot_idx) → value: JSON list of car_ids
        self.reservation_board = runtime_context.get_map_state(
            MapStateDescriptor("reservation_board", Types.STRING(), Types.STRING()))

        # key: car_id → value: JSON {"reserved_slots": [...], "planned_soc": float, "planned_at_idx": int}
        self.car_reservations = runtime_context.get_map_state(
            MapStateDescriptor("car_reservations", Types.STRING(), Types.STRING()))

    # -----------------------------------------------------------------------
    def process_element(self, value, ctx: KeyedProcessFunction.Context):
        event_type, data = value

        if event_type == 'PRICE':
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
    # Helper: load reservation_counts dict from reservation_board state
    # -----------------------------------------------------------------------
    def _load_reservation_counts(self):
        """Return {slot_idx(int): count(int)} from the reservation board."""
        counts = {}
        for slot_key in self.reservation_board.keys():
            car_list_json = self.reservation_board.get(slot_key)
            if car_list_json:
                car_list = json.loads(car_list_json)
                counts[int(slot_key)] = len(car_list)
        return counts

    def _remove_car_from_board(self, car_id, slots):
        """Remove car_id from the given slots on the reservation board."""
        for slot in slots:
            slot_key = str(slot)
            car_list_json = self.reservation_board.get(slot_key)
            if car_list_json:
                car_list = json.loads(car_list_json)
                if car_id in car_list:
                    car_list.remove(car_id)
                if car_list:
                    self.reservation_board.put(slot_key, json.dumps(car_list))
                else:
                    self.reservation_board.remove(slot_key)

    def _add_car_to_board(self, car_id, slots):
        """Add car_id to the given slots on the reservation board."""
        for slot in slots:
            slot_key = str(slot)
            car_list_json = self.reservation_board.get(slot_key)
            if car_list_json:
                car_list = json.loads(car_list_json)
            else:
                car_list = []
            if car_id not in car_list:
                car_list.append(car_id)
            self.reservation_board.put(slot_key, json.dumps(car_list))

    # -----------------------------------------------------------------------
    def on_timer(self, timestamp, ctx: KeyedProcessFunction.OnTimerContext):
        idx = self.interval_state.value()
        if idx is None:
            idx = 0

        # Midnight roll-over: advance day_of_week, clear reservations
        if idx >= 96:
            dow = (self.day_of_week_state.value() or 0) + 1
            self.day_of_week_state.update(dow % 7)
            log('SYSTEM', f'MIDNIGHT — day_of_week now {dow % 7}')

            # Clear reservation state
            for slot_key in list(self.reservation_board.keys()):
                self.reservation_board.remove(slot_key)
            for car_id in list(self.car_reservations.keys()):
                self.car_reservations.remove(car_id)
            log('RESERVATION', 'Midnight reset — cleared all reservations')

            idx = 0

        self.interval_state.update(idx + 1)
        day_of_week = self.day_of_week_state.value() or 0

        # Current dynamic price (fall back to base if no message received yet)
        current_price = self.current_price_state.value()
        if current_price is None:
            current_price = BASE_PRICE

        car_ids = list(self.car_state.keys())

        # Extract cars and shuffle/sort by SOC (lowest first)
        car_list = []
        for cid in car_ids:
            cdr = self.car_state.get(cid)
            if cdr:
                car_list.append((cid, json.loads(cdr)))

        import random
        random.shuffle(car_list)
        car_list.sort(key=lambda x: x[1].get('current_soc', 1.0))

        # ===================================================================
        # PHASE 1 — PLANNING: Update reservations for cars
        # ===================================================================
        for car_id, car_data in car_list:
            current_soc = car_data.get('current_soc', 1.0)
            is_emergency = current_soc < 0.15

            # Load existing reservation for this car
            res_json = self.car_reservations.get(car_id)
            car_res = json.loads(res_json) if res_json else None

            if not should_replan(car_res, idx, current_soc):
                continue

            # Remove car from old slots on the board
            if car_res and car_res.get('reserved_slots'):
                self._remove_car_from_board(car_id, car_res['reserved_slots'])

            # How many slots does this car need?
            num_needed = slots_needed(current_soc)
            if num_needed <= 0:
                # Car is at or above target — clear reservation
                self.car_reservations.remove(car_id)
                continue

            # Build reservation counts from the board (reflects all other cars' reservations)
            reservation_counts = self._load_reservation_counts()

            # Select cheapest slots
            chosen_slots = select_cheapest_slots(idx, num_needed, reservation_counts, is_emergency)

            if not chosen_slots:
                self.car_reservations.remove(car_id)
                continue

            # Update the board and car reservations
            self._add_car_to_board(car_id, chosen_slots)
            new_res = {
                'reserved_slots': chosen_slots,
                'planned_soc': current_soc,
                'planned_at_idx': idx,
            }
            self.car_reservations.put(car_id, json.dumps(new_res))
            log('RESERVATION', f'{car_id} | soc={current_soc*100:.1f}% | slots={chosen_slots}')

        # ===================================================================
        # PHASE 2 — EXECUTION: Allocate chargers by priority
        # ===================================================================
        # Build set of car_ids that have a reservation for this slot
        slot_key = str(idx)
        reserved_this_slot_json = self.reservation_board.get(slot_key)
        reserved_this_slot = set(json.loads(reserved_this_slot_json)) if reserved_this_slot_json else set()

        # Categorize cars into priority buckets
        emergency_cars = []
        reserved_cars = []
        opportunistic_cars = []

        for car_id, car_data in car_list:
            current_soc = car_data.get('current_soc', 1.0)
            is_emergency = current_soc < 0.15
            recovering = car_data.get('emergency_charging', False) and current_soc < 0.40

            if is_emergency or recovering:
                emergency_cars.append((car_id, car_data))
            elif car_id in reserved_this_slot:
                reserved_cars.append((car_id, car_data))
            else:
                # Check heuristic fallback
                prev_charging = self.was_charging.get(car_id) or False
                if decide_charge(car_data, current_price, prev_charging):
                    opportunistic_cars.append((car_id, car_data))
                else:
                    opportunistic_cars.append((car_id, car_data))  # still in list for command generation

        # Build the priority-ordered charging queue
        # Emergency → Reserved → Opportunistic (only those that want to charge)
        total_busy = 0
        new_real_active = 0

        # Track which cars get to charge and their reasons
        charge_decisions = {}  # car_id → (should_charge, reason_str, car_data)

        # 1. Emergency cars — always charge
        for car_id, car_data in emergency_cars:
            current_soc = car_data.get('current_soc', 1.0)
            if current_soc >= 0.99:
                charge_decisions[car_id] = (False, "full", car_data)
                continue
            should_charge = total_busy < STATION_CAPACITY
            if should_charge:
                total_busy += 1
                new_real_active += 1
                car_data['emergency_charging'] = True
                charge_decisions[car_id] = (True, "emergency", car_data)
                log('FLINK', f'{car_id} | EMERGENCY | soc={current_soc*100:.1f}% | price={current_price:.2f}')
            else:
                car_data['emergency_charging'] = False
                charge_decisions[car_id] = (False, "station full", car_data)
                log('FLINK', f'{car_id} | EMERGENCY DEFERRED (station full) | soc={current_soc*100:.1f}%')

        # 2. Reserved cars — have a reservation for this slot
        for car_id, car_data in reserved_cars:
            current_soc = car_data.get('current_soc', 1.0)
            if current_soc >= 0.99:
                charge_decisions[car_id] = (False, "full", car_data)
                continue
            should_charge = total_busy < STATION_CAPACITY
            if should_charge:
                total_busy += 1
                new_real_active += 1
                car_data['emergency_charging'] = False
                charge_decisions[car_id] = (True, "reserved", car_data)
                log('FLINK', f'{car_id} | RESERVED CHARGING | soc={current_soc*100:.1f}% | price={current_price:.2f}')
            else:
                car_data['emergency_charging'] = False
                charge_decisions[car_id] = (False, "station full", car_data)
                log('FLINK', f'{car_id} | RESERVED DEFERRED (station full) | soc={current_soc*100:.1f}%')

        # 3. Opportunistic real cars — heuristic fallback
        for car_id, car_data in opportunistic_cars:
            if car_id in charge_decisions:
                continue  # already handled as emergency or reserved
            current_soc = car_data.get('current_soc', 1.0)
            prev_charging = self.was_charging.get(car_id) or False
            car_wants_to_charge = decide_charge(car_data, current_price, prev_charging)

            if current_soc >= 0.99:
                charge_decisions[car_id] = (False, "full", car_data)
                continue

            if car_wants_to_charge:
                should_charge = total_busy < STATION_CAPACITY
                if should_charge:
                    total_busy += 1
                    new_real_active += 1
                    car_data['emergency_charging'] = False
                    charge_decisions[car_id] = (True, "opportunistic", car_data)
                    log('FLINK', f'{car_id} | OPPORTUNISTIC CHARGING | soc={current_soc*100:.1f}% | price={current_price:.2f}')
                else:
                    car_data['emergency_charging'] = False
                    charge_decisions[car_id] = (False, "station full", car_data)
            else:
                car_data['emergency_charging'] = False
                charge_decisions[car_id] = (False, "price too high", car_data)

        # Handle any cars not yet in charge_decisions (shouldn't happen, but guard)
        for car_id, car_data in car_list:
            if car_id not in charge_decisions:
                charge_decisions[car_id] = (False, "idle", car_data)

        # ===================================================================
        # YIELD COMMANDS for all cars
        # ===================================================================
        for car_id, car_data in car_list:
            should_charge, reason_str, car_data = charge_decisions[car_id]
            target_power = MAX_POWER_PER_CHARGER if should_charge else 0.0

            self.was_charging.put(car_id, should_charge)

            # Get reservation info for this car
            has_reservation = car_id in reserved_this_slot
            res_json = self.car_reservations.get(car_id)
            car_reserved_slots = json.loads(res_json).get('reserved_slots', []) if res_json else []

            command = {
                'car_id':         car_id,
                'interval_idx':   idx,
                'day_of_week':    day_of_week,
                'action':         'START_CHARGING' if should_charge else 'STOP_CHARGING',
                'reason':         reason_str,
                'power_kw':       target_power,
                'timestamp':      time.time(),
                'charging_price': round(current_price, 4) if should_charge else None,
                'reserved':       has_reservation,
                'reserved_slots': car_reserved_slots,
            }

            if should_charge:
                energy_added = target_power * 0.25  # 15-sim-min = 0.25h
                new_kwh = min(60.0, car_data.get('current_battery_kwh', 60.0) + energy_added)
                new_kwh = max(new_kwh, car_data.get('current_battery_kwh', 60.0))
                car_data['current_battery_kwh'] = new_kwh
                car_data['current_soc'] = new_kwh / 60.0
                car_data['plugged_in'] = True
                car_data['flink_managed'] = True
                self.car_state.put(car_id, json.dumps(car_data))
                command['new_soc'] = car_data['current_soc']
            else:
                if car_data.get('plugged_in', False):
                    car_data['plugged_in'] = False
                    car_data['flink_managed'] = False
                    self.car_state.put(car_id, json.dumps(car_data))
                real_soc = car_data.get('current_soc')
                command['new_soc'] = real_soc if (real_soc is not None and real_soc > 0.01) else None

            log_command(car_id, command['action'], command.get('new_soc'), idx)
            yield json.dumps(command)

        # ===================================================================
        # EMIT RESERVATION STATUS
        # ===================================================================
        reservation_counts = self._load_reservation_counts()
        projected_prices = [
            projected_price(reservation_counts.get(s, 0))
            for s in range(SLOTS_PER_DAY)
        ]
        reservation_status = {
            'type':               'RESERVATION_STATUS',
            'car_id':             '__reservation_status__',
            'interval_idx':       idx,
            'reservation_counts': {str(k): v for k, v in reservation_counts.items()},
            'projected_prices':   [round(p, 4) for p in projected_prices],
            'timestamp':          time.time(),
        }
        yield json.dumps(reservation_status)

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
    env.add_python_file(os.path.join(project_root, "optimizers", "reservation_scheduler.py"))
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
