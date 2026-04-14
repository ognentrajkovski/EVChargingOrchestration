"""
EV Fleet Bidding Orchestrator (Flink) — Multi-Station Edition
=============================================================
Two-phase per-tick processing:

  Phase 1 — Planning:
    Each car picks the (station, slots) combination that minimises
    total_EUR = travel_cost + charging_cost using the NetworkX fleet graph.
    Cars are processed lowest-SOC-first so urgent cars get first pick.
    Emergency cars go to the nearest available station (minimum distance).

  Phase 2 — Execution:
    Allocate physical chargers by priority:
      1. Emergency   (SOC < 15%, or recovering to 50%)
      2. Reserved    (car has a reservation for this slot at its chosen station)
      3. Opportunistic (heuristic fallback)
    Per-station capacity is enforced independently.

  RESERVATION_STATUS is emitted after execution so producers + dashboard
  can display projected prices and reservation counts per station.

Reservation board key format: "{station_id}:{slot_idx}"  (e.g. "station_A:47")
"""

import json
import logging
import math
import sys
import os
import time

project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.append(os.path.join(project_root, "optimizers"))
sys.path.append(os.path.join(project_root, "config"))

from pyflink.common import Types, SimpleStringSchema
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.datastream.connectors.kafka import (
    KafkaSource, KafkaSink, KafkaRecordSerializationSchema, DeliveryGuarantee,
)
from pyflink.datastream.functions import KeyedProcessFunction, RuntimeContext
from pyflink.datastream.state import ValueStateDescriptor, MapStateDescriptor
from pyflink.common import WatermarkStrategy

from heuristic_scheduler import decide_charge, BASE_PRICE, MAX_POWER_PER_CHARGER
from reservation_scheduler import should_replan, slots_needed, SLOTS_PER_DAY
from fleet_graph import (
    build_fleet_graph,
    select_station_and_slots,
    nearest_available_station,
    get_cars_reserved_at_slot,
    get_reservation_summary,
)
from stations import STATIONS, projected_price_for_station, ARRIVE_THRESHOLD_KM
from ai_scheduler import QLearningAgent

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
TICK_MS           = 1250


class EVFleetProcessor(KeyedProcessFunction):

    def open(self, runtime_context: RuntimeContext):
        # --- Car state ---
        self.car_state = runtime_context.get_map_state(
            MapStateDescriptor("cars", Types.STRING(), Types.STRING()))

        # --- Per-station prices (station_id → float) ---
        self.station_prices = runtime_context.get_map_state(
            MapStateDescriptor("station_prices", Types.STRING(), Types.FLOAT()))

        # --- Heartbeat / interval ---
        self.timer_set_state = runtime_context.get_state(
            ValueStateDescriptor("timer_set", Types.BOOLEAN()))
        self.interval_state = runtime_context.get_state(
            ValueStateDescriptor("interval_idx", Types.INT()))

        # --- Day-of-week (0=Mon … 6=Sun) ---
        self.day_of_week_state = runtime_context.get_state(
            ValueStateDescriptor("day_of_week", Types.INT()))

        # --- Per-car charging tracking ---
        self.was_charging = runtime_context.get_map_state(
            MapStateDescriptor("was_charging", Types.STRING(), Types.BOOLEAN()))

        # --- Per-car Q-learning agents (one per AI car, created on first decision) ---
        self._q_agents: dict = {}  # car_id → QLearningAgent

        # --- Reservation board ---
        # key: "{station_id}:{slot_idx}"  →  value: JSON list of car_ids
        self.reservation_board = runtime_context.get_map_state(
            MapStateDescriptor("reservation_board", Types.STRING(), Types.STRING()))

        # --- Per-car reservations ---
        # key: car_id  →  value: JSON {station_id, reserved_slots, planned_soc, planned_at_idx}
        self.car_reservations = runtime_context.get_map_state(
            MapStateDescriptor("car_reservations", Types.STRING(), Types.STRING()))

    # -----------------------------------------------------------------------
    def process_element(self, value, ctx: KeyedProcessFunction.Context):
        event_type, data = value

        if event_type == 'PRICE':
            # Multi-station: update per-station price map
            stations_data = data.get('stations', {})
            for sid, sdata in stations_data.items():
                price = sdata.get('current_price')
                if price is not None:
                    self.station_prices.put(sid, float(price))
            # Backward-compat: single price field
            if not stations_data and data.get('current_price') is not None:
                self.station_prices.put('station_A', float(data['current_price']))

        elif event_type == 'CAR':
            car_id = data.get('id')
            if not car_id:
                return
            existing_str = self.car_state.get(car_id)
            if existing_str:
                existing = json.loads(existing_str)
                flink_owns = existing.get('flink_managed', False)
                if flink_owns:
                    existing['priority']   = data.get('priority', existing['priority'])
                    existing['plugged_in'] = data.get('plugged_in', False)
                    # Always update position (car moves even while managed)
                    existing['x'] = data.get('x', existing.get('x', 5.0))
                    existing['y'] = data.get('y', existing.get('y', 5.0))
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
    # Reservation board helpers  (composite key: "{station_id}:{slot_idx}")
    # -----------------------------------------------------------------------

    def _load_station_reservation_counts(self) -> dict:
        """Return {station_id: {slot_idx(int): count(int)}} from the board."""
        result = {sid: {} for sid in STATIONS}
        for key in self.reservation_board.keys():
            if ':' not in key:
                continue
            sid, slot_str = key.split(':', 1)
            if sid not in result:
                continue
            car_list_json = self.reservation_board.get(key)
            if car_list_json:
                result[sid][int(slot_str)] = len(json.loads(car_list_json))
        return result

    def _load_all_car_reservations(self) -> dict:
        """Return {car_id: reservation_dict} for all cars."""
        result = {}
        for car_id in self.car_reservations.keys():
            res_json = self.car_reservations.get(car_id)
            if res_json:
                result[car_id] = json.loads(res_json)
        return result

    def _remove_car_from_board(self, car_id: str, station_id: str, slots: list):
        for slot in slots:
            key = f"{station_id}:{slot}"
            car_list_json = self.reservation_board.get(key)
            if car_list_json:
                car_list = json.loads(car_list_json)
                if car_id in car_list:
                    car_list.remove(car_id)
                if car_list:
                    self.reservation_board.put(key, json.dumps(car_list))
                else:
                    self.reservation_board.remove(key)

    def _add_car_to_board(self, car_id: str, station_id: str, slots: list):
        for slot in slots:
            key = f"{station_id}:{slot}"
            car_list_json = self.reservation_board.get(key)
            car_list = json.loads(car_list_json) if car_list_json else []
            if car_id not in car_list:
                car_list.append(car_id)
            self.reservation_board.put(key, json.dumps(car_list))

    def _select_emergency_slots(
        self, station_id: str, current_idx: int, num_needed: int,
        station_res_counts: dict,
    ) -> list:
        """Consecutive slots for emergency cars at a chosen station."""
        counts   = station_res_counts.get(station_id, {})
        capacity = STATIONS[station_id]['capacity']
        selected = []
        for slot in range(current_idx, SLOTS_PER_DAY):
            if len(selected) >= num_needed:
                break
            if counts.get(slot, 0) < capacity:
                selected.append(slot)
        return selected

    def _select_cheapest_slots_at_station(
        self,
        station_id: str,
        current_idx: int,
        num_needed: int,
        station_res_counts: dict,
    ) -> list:
        """
        Select the cheapest available future slots within a single pre-chosen
        station. Used by the Q-learning agent after it picks a station.
        """
        counts   = station_res_counts.get(station_id, {})
        capacity = STATIONS[station_id]['capacity']
        candidates = []
        for slot in range(current_idx, SLOTS_PER_DAY):
            cnt = counts.get(slot, 0)
            if cnt >= capacity:
                continue
            price = projected_price_for_station(cnt, station_id)
            candidates.append((price, slot))
        candidates.sort()
        return sorted(slot for _, slot in candidates[:num_needed])

    # -----------------------------------------------------------------------
    def on_timer(self, timestamp, ctx: KeyedProcessFunction.OnTimerContext):
        idx = self.interval_state.value()
        if idx is None:
            idx = 0

        # Midnight roll-over
        if idx >= 96:
            dow = (self.day_of_week_state.value() or 0) + 1
            self.day_of_week_state.update(dow % 7)
            log('SYSTEM', f'MIDNIGHT — day_of_week now {dow % 7}')
            for key in list(self.reservation_board.keys()):
                self.reservation_board.remove(key)
            for car_id in list(self.car_reservations.keys()):
                self.car_reservations.remove(car_id)
            log('RESERVATION', 'Midnight reset — cleared all reservations')
            idx = 0

        self.interval_state.update(idx + 1)
        day_of_week = self.day_of_week_state.value() or 0

        # Remove stale entries for the just-passed slot
        if idx > 0:
            for sid in STATIONS:
                past_key = f"{sid}:{idx - 1}"
                if self.reservation_board.contains(past_key):
                    self.reservation_board.remove(past_key)

        # Build car list sorted by SOC (lowest first)
        car_ids  = list(self.car_state.keys())
        car_list = []
        for cid in car_ids:
            cdr = self.car_state.get(cid)
            if cdr:
                car_list.append((cid, json.loads(cdr)))
        import random
        random.shuffle(car_list)
        car_list.sort(key=lambda x: x[1].get('current_soc', 1.0))

        cars_dict = {cid: cdata for cid, cdata in car_list}

        # ===================================================================
        # PHASE 1 — PLANNING
        # ===================================================================
        for car_id, car_data in car_list:
            current_soc  = car_data.get('current_soc', 1.0)
            is_emergency = current_soc < 0.15

            res_json = self.car_reservations.get(car_id)
            car_res  = json.loads(res_json) if res_json else None

            if not should_replan(car_res, idx, current_soc):
                continue

            # Remove from old station's board
            if car_res and car_res.get('reserved_slots') and car_res.get('station_id'):
                self._remove_car_from_board(
                    car_id, car_res['station_id'], car_res['reserved_slots'])

            num_needed = slots_needed(current_soc)
            if num_needed <= 0:
                self.car_reservations.remove(car_id)
                continue

            # Rebuild graph so this car sees all previous replans this tick
            station_res_counts  = self._load_station_reservation_counts()
            car_res_snapshot    = self._load_all_car_reservations()
            G = build_fleet_graph(
                cars_dict, station_res_counts, idx, car_reservations=car_res_snapshot)

            if is_emergency:
                chosen_station = nearest_available_station(G, car_id, idx)
                chosen_slots   = (
                    self._select_emergency_slots(
                        chosen_station, idx, num_needed, station_res_counts)
                    if chosen_station else []
                )
            elif car_data.get('group') == 'ai':
                # ── Per-car Q-Learning station selection (ai group) ──────────
                # Each AI car has its own agent — created on first decision.
                if car_id not in self._q_agents:
                    self._q_agents[car_id] = QLearningAgent()
                q_agent = self._q_agents[car_id]

                station_prices_dict = {
                    sid: (self.station_prices.get(sid) or STATIONS[sid]['base_price'])
                    for sid in STATIONS
                }
                # Extract per-station distances from the NetworkX graph
                station_distances = {}
                for sid in STATIONS:
                    edge = G.get_edge_data(car_id, sid)
                    station_distances[sid] = edge['distance_km'] if edge else 5.0

                top3_stations  = q_agent.get_top3_stations(station_distances)
                state          = q_agent.encode_state(
                    current_soc, station_prices_dict, station_res_counts,
                    top3_stations, station_distances, current_idx=idx)
                action         = q_agent.select_action(state)
                # Resolve positional index to an actual station ID
                chosen_station = (top3_stations[action]
                                  if action is not None and action < len(top3_stations)
                                  else None)
                chosen_slots   = (
                    self._select_cheapest_slots_at_station(
                        chosen_station, idx, num_needed, station_res_counts)
                    if chosen_station else []
                )
                # Q-update: observe next state after booking
                next_res_counts = self._load_station_reservation_counts()
                next_state      = q_agent.encode_state(
                    current_soc, station_prices_dict, next_res_counts,
                    top3_stations, station_distances, current_idx=idx)
                reward = q_agent.compute_reward(
                    chosen_station, chosen_slots, station_prices_dict,
                    current_soc, car_data)
                q_agent.update(state, action, reward, next_state)
            else:
                # ── Heuristic station selection (heuristic group) ────────────
                chosen_station, chosen_slots = select_station_and_slots(
                    G, car_id, num_needed, idx)

            if not chosen_slots or not chosen_station:
                self.car_reservations.remove(car_id)
                continue

            self._add_car_to_board(car_id, chosen_station, chosen_slots)
            new_res = {
                'station_id':     chosen_station,
                'reserved_slots': chosen_slots,
                'planned_soc':    current_soc,
                'planned_at_idx': idx,
            }
            self.car_reservations.put(car_id, json.dumps(new_res))
            log('RESERVATION',
                f'{car_id} | soc={current_soc*100:.1f}% | '
                f'station={chosen_station} | slots={chosen_slots}')

            # Tell the car producer to start moving toward the assigned station
            stn = STATIONS[chosen_station]
            yield json.dumps({
                'car_id':      car_id,
                'group':       car_data.get('group', 'heuristic'),
                'action':      'SET_TARGET',
                'station_id':  chosen_station,
                'station_x':   stn['x'],
                'station_y':   stn['y'],
                'timestamp':   time.time(),
            })

        # ===================================================================
        # PHASE 2 — EXECUTION
        # Build the graph once after all replanning is done
        # ===================================================================
        station_res_counts = self._load_station_reservation_counts()
        car_res_snapshot   = self._load_all_car_reservations()
        G = build_fleet_graph(
            cars_dict, station_res_counts, idx, car_reservations=car_res_snapshot)

        # Which cars have a reservation for THIS slot, and at which station?
        reserved_this_slot = get_cars_reserved_at_slot(G, idx)

        # Per-station capacity tracking
        station_busy = {sid: 0 for sid in STATIONS}

        # Categorise into priority buckets
        emergency_cars    = []
        reserved_cars     = []
        opportunistic_cars = []

        for car_id, car_data in car_list:
            current_soc  = car_data.get('current_soc', 1.0)
            is_emergency = current_soc < 0.15
            recovering   = car_data.get('emergency_charging', False) and current_soc < 0.50

            if is_emergency or recovering:
                emergency_cars.append((car_id, car_data))
            elif car_id in reserved_this_slot:
                reserved_cars.append((car_id, car_data))
            else:
                opportunistic_cars.append((car_id, car_data))

        charge_decisions = {}  # car_id → (should_charge, reason, car_data, station_id)

        # 1. Emergency
        for car_id, car_data in emergency_cars:
            current_soc = car_data.get('current_soc', 1.0)
            if current_soc >= 0.99:
                charge_decisions[car_id] = (False, "full", car_data, None)
                continue
            # Use car's reserved station or find nearest available
            res_json = self.car_reservations.get(car_id)
            car_res  = json.loads(res_json) if res_json else None
            sid      = car_res.get('station_id') if car_res else None
            if sid is None or station_busy.get(sid, 0) >= STATIONS.get(sid, {}).get('capacity', 0):
                sid = nearest_available_station(G, car_id, idx)
            if sid and station_busy[sid] < STATIONS[sid]['capacity']:
                station_busy[sid] += 1
                car_data['emergency_charging'] = True
                charge_decisions[car_id] = (True, "emergency", car_data, sid)
                log('FLINK', f'{car_id} | EMERGENCY → {sid} | soc={current_soc*100:.1f}%')
            else:
                car_data['emergency_charging'] = False
                charge_decisions[car_id] = (False, "station full", car_data, sid)

        # 2. Reserved
        for car_id, car_data in reserved_cars:
            current_soc = car_data.get('current_soc', 1.0)
            if current_soc >= 0.99:
                charge_decisions[car_id] = (False, "full", car_data, None)
                continue
            sid = reserved_this_slot.get(car_id)
            if sid:
                # Check car has physically arrived at the station
                stn   = STATIONS[sid]
                car_x = car_data.get('x', 5.0)
                car_y = car_data.get('y', 5.0)
                dist  = math.sqrt((car_x - stn['x'])**2 + (car_y - stn['y'])**2)
                if dist > ARRIVE_THRESHOLD_KM:
                    car_data['emergency_charging'] = False
                    charge_decisions[car_id] = (False, "in_transit", car_data, sid)
                    log('FLINK', f'{car_id} | IN_TRANSIT → {sid} | dist={dist:.2f}km')
                    continue
            if sid and station_busy[sid] < STATIONS[sid]['capacity']:
                station_busy[sid] += 1
                car_data['emergency_charging'] = False
                charge_decisions[car_id] = (True, "reserved", car_data, sid)
                log('FLINK', f'{car_id} | RESERVED → {sid} | soc={current_soc*100:.1f}%')
            else:
                car_data['emergency_charging'] = False
                charge_decisions[car_id] = (False, "station full", car_data, sid)

        # 3. Opportunistic — pick nearest station with free capacity
        for car_id, car_data in opportunistic_cars:
            if car_id in charge_decisions:
                continue
            current_soc  = car_data.get('current_soc', 1.0)
            prev_charging = self.was_charging.get(car_id) or False
            # Use the car's planned station price as proxy; fallback to station_A
            res_json = self.car_reservations.get(car_id)
            car_res  = json.loads(res_json) if res_json else None
            planned_sid = car_res.get('station_id') if car_res else 'station_A'
            proxy_price = (self.station_prices.get(planned_sid)
                           or STATIONS.get(planned_sid, {}).get('base_price', BASE_PRICE))

            if current_soc >= 0.99:
                charge_decisions[car_id] = (False, "full", car_data, None)
                continue

            # If the car has already arrived at its planned station, charge
            # immediately — don't wait for the reserved slot tick or run a
            # price check. The car travelled here; use the charger.
            car_x = car_data.get('x', 5.0)
            car_y = car_data.get('y', 5.0)
            if planned_sid and current_soc < 0.99:
                stn_p = STATIONS.get(planned_sid, {})
                dist_to_planned = math.sqrt(
                    (car_x - stn_p.get('x', 0))**2 + (car_y - stn_p.get('y', 0))**2)
                if (dist_to_planned <= ARRIVE_THRESHOLD_KM
                        and station_busy.get(planned_sid, 0) < stn_p.get('capacity', 0)):
                    station_busy[planned_sid] += 1
                    car_data['emergency_charging'] = False
                    charge_decisions[car_id] = (True, "arrived", car_data, planned_sid)
                    log('FLINK', f'{car_id} | ARRIVED → {planned_sid} | soc={current_soc*100:.1f}%')
                    continue

            car_wants_to_charge = decide_charge(car_data, proxy_price, prev_charging)

            if car_wants_to_charge:
                # Pick nearest station with free capacity
                car_x = car_data.get('x', 5.0)
                car_y = car_data.get('y', 5.0)
                best_sid  = None
                best_dist = float('inf')
                for sid, stn in STATIONS.items():
                    if station_busy[sid] >= stn['capacity']:
                        continue
                    dist = math.sqrt((car_x - stn['x'])**2 + (car_y - stn['y'])**2)
                    if dist < best_dist:
                        best_dist = dist
                        best_sid  = sid
                if best_sid:
                    station_busy[best_sid] += 1
                    car_data['emergency_charging'] = False
                    charge_decisions[car_id] = (True, "opportunistic", car_data, best_sid)
                    log('FLINK', f'{car_id} | OPPORTUNISTIC → {best_sid} | soc={current_soc*100:.1f}%')
                else:
                    charge_decisions[car_id] = (False, "station full", car_data, None)
            else:
                charge_decisions[car_id] = (False, "price too high", car_data, None)

        # Guard: any cars not yet decided
        for car_id, car_data in car_list:
            if car_id not in charge_decisions:
                charge_decisions[car_id] = (False, "idle", car_data, None)

        # ===================================================================
        # YIELD COMMANDS + track per-group metrics
        # ===================================================================
        new_station_active = {sid: 0 for sid in STATIONS}

        # Per-group metrics accumulated this tick
        group_metrics = {
            'heuristic': {'charge_count': 0, 'emergency_count': 0,
                          'total_cost_eur': 0.0, 'soc_sum': 0.0, 'car_count': 0},
            'ai':        {'charge_count': 0, 'emergency_count': 0,
                          'total_cost_eur': 0.0, 'soc_sum': 0.0, 'car_count': 0},
        }

        for car_id, car_data in car_list:
            should_charge, reason_str, car_data, sid = charge_decisions[car_id]
            target_power = MAX_POWER_PER_CHARGER if should_charge else 0.0
            car_group    = car_data.get('group', 'heuristic')

            self.was_charging.put(car_id, should_charge)
            if should_charge and sid:
                new_station_active[sid] += 1

            # Reservation info
            has_reservation   = car_id in reserved_this_slot
            res_json          = self.car_reservations.get(car_id)
            car_reserved_slots = (json.loads(res_json).get('reserved_slots', [])
                                   if res_json else [])
            # Station price for this command
            station_price = (
                (self.station_prices.get(sid) or STATIONS.get(sid, {}).get('base_price', BASE_PRICE))
                if sid else BASE_PRICE
            )

            stn_coords = STATIONS.get(sid, {}) if sid else {}
            command = {
                'car_id':         car_id,
                'group':          car_group,
                'station_id':     sid,
                'station_x':      stn_coords.get('x'),
                'station_y':      stn_coords.get('y'),
                'interval_idx':   idx,
                'day_of_week':    day_of_week,
                'action':         'START_CHARGING' if should_charge else 'STOP_CHARGING',
                'reason':         reason_str,
                'power_kw':       target_power,
                'timestamp':      time.time(),
                'charging_price': round(station_price, 4) if should_charge else None,
                'reserved':       has_reservation,
                'reserved_slots': car_reserved_slots,
            }

            if should_charge:
                energy_added = target_power * 0.25
                new_kwh = min(60.0, car_data.get('current_battery_kwh', 60.0) + energy_added)
                new_kwh = max(new_kwh, car_data.get('current_battery_kwh', 60.0))
                car_data['current_battery_kwh'] = new_kwh
                car_data['current_soc']          = new_kwh / 60.0
                car_data['plugged_in']            = True
                car_data['flink_managed']         = True
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

            # ── Per-group metrics ─────────────────────────────────────────
            grp = group_metrics.get(car_group, group_metrics['heuristic'])
            grp['car_count'] += 1
            grp['soc_sum']   += car_data.get('current_soc', 0.0)
            if should_charge and station_price:
                grp['charge_count']   += 1
                grp['total_cost_eur'] += station_price * 2.75 / 1000
            if 'emergency' in reason_str.lower():
                grp['emergency_count'] += 1

        # ===================================================================
        # EMIT RESERVATION STATUS
        # ===================================================================
        station_res_counts = self._load_station_reservation_counts()

        # Per-station projected prices and counts
        per_station_data = {}
        flat_counts      = {}
        flat_projected   = []

        for sid, stn in STATIONS.items():
            counts = station_res_counts.get(sid, {})
            s_projected = [
                round(projected_price_for_station(counts.get(s, 0), sid), 4)
                for s in range(SLOTS_PER_DAY)
            ]
            s_counts_str = {str(k): v for k, v in counts.items()}
            per_station_data[sid] = {
                'reservation_counts': s_counts_str,
                'projected_prices':   s_projected,
                'capacity':           stn['capacity'],
                'base_price':         stn['base_price'],
            }
            for k, v in counts.items():
                flat_counts[str(k)] = flat_counts.get(str(k), 0) + v

        # Aggregate projected prices (average across stations weighted by capacity)
        total_cap = sum(s['capacity'] for s in STATIONS.values())
        flat_projected = []
        for s in range(SLOTS_PER_DAY):
            avg = sum(
                per_station_data[sid]['projected_prices'][s] * STATIONS[sid]['capacity']
                for sid in STATIONS
            ) / max(total_cap, 1)
            flat_projected.append(round(avg, 4))

        reservation_status = {
            'type':                              'RESERVATION_STATUS',
            'car_id':                            '__reservation_status__',
            'interval_idx':                      idx,
            'stations':                          per_station_data,
            'reservation_counts_per_station':    {sid: {str(k): v for k, v in station_res_counts.get(sid, {}).items()} for sid in STATIONS},
            # Backward-compat flat fields
            'reservation_counts':                flat_counts,
            'projected_prices':                  flat_projected,
            'timestamp':                         time.time(),
            'group_metrics': {
                grp: {
                    'charge_count':   m['charge_count'],
                    'emergency_count': m['emergency_count'],
                    'avg_cost_eur':   round(m['total_cost_eur'] / m['charge_count'], 5)
                                      if m['charge_count'] else 0.0,
                    'avg_soc':        round(m['soc_sum'] / m['car_count'], 4)
                                      if m['car_count'] else 0.0,
                    'car_count':      m['car_count'],
                }
                for grp, m in group_metrics.items()
            },
            'q_agent_stats': {
                'epsilon': round(
                    sum(a.epsilon for a in self._q_agents.values()) / len(self._q_agents)
                    if self._q_agents else 0.80, 3),
                'steps': sum(a.steps for a in self._q_agents.values()),
                'reward_history': sorted(
                    [r for a in self._q_agents.values()
                       for r in a.reward_history[-5:]],
                )[-50:],
                'agent_count': len(self._q_agents),
                'per_car': {
                    car_id: {
                        'epsilon':     round(agent.epsilon, 3),
                        'steps':       agent.steps,
                        'last_reward': agent.reward_history[-1]
                                       if agent.reward_history else 0.0,
                        'avg_reward':  round(
                            sum(agent.reward_history[-20:]) / len(agent.reward_history[-20:]), 3)
                                       if agent.reward_history else 0.0,
                    }
                    for car_id, agent in self._q_agents.items()
                },
            },
        }
        yield json.dumps(reservation_status)

        ctx.timer_service().register_processing_time_timer(timestamp + TICK_MS)


# ---------------------------------------------------------------------------
def main():
    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_parallelism(1)
    env.enable_checkpointing(10000)

    project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    env.add_python_file(os.path.join(project_root, "optimizers", "heuristic_scheduler.py"))
    env.add_python_file(os.path.join(project_root, "optimizers", "reservation_scheduler.py"))
    env.add_python_file(os.path.join(project_root, "optimizers", "fleet_graph.py"))
    env.add_python_file(os.path.join(project_root, "optimizers", "ai_scheduler.py"))
    env.add_python_file(os.path.join(project_root, "config", "stations.py"))
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

    ds_energy   = env.from_source(energy_source, WatermarkStrategy.no_watermarks(), "Energy").map(
        lambda x: ('PRICE', json.loads(x)))
    ds_cars     = env.from_source(car_source, WatermarkStrategy.no_watermarks(), "Cars").map(
        lambda x: ('CAR', json.loads(x)))

    ds_keyed    = ds_energy.union(ds_cars).key_by(lambda x: "global")
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
    env.execute("EV Fleet Multi-Station Bidding Orchestrator")


if __name__ == '__main__':
    main()
