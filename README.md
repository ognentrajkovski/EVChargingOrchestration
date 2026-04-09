# Real-Time Cost Optimization Engine for Autonomous EV Fleets

A production-grade streaming system that orchestrates a fleet of **80 autonomous electric vehicles** (40 heuristic + 40 AI) across 3 charging stations in real time. Apache Flink processes every vehicle tick, builds a NetworkX knowledge graph, and routes each car to the station that minimises the combined **travel cost + charging cost** — not just the cheapest or nearest station. A Q-Learning AI agent runs in parallel with the heuristic, and a three-view dashboard compares both systems live.

---

## Table of Contents

- [Architecture Overview](#architecture-overview)
- [Quick Start](#quick-start)
- [Project Structure](#project-structure)
- [System Components](#system-components)
  - [Configuration — `config/stations.py`](#configuration--configstationspy)
  - [Knowledge Graph — `optimizers/fleet_graph.py`](#knowledge-graph--optimizersfleet_graphpy)
  - [Reservation Scheduler — `optimizers/reservation_scheduler.py`](#reservation-scheduler--optimizersreservation_schedulerpy)
  - [AI Scheduler — `optimizers/ai_scheduler.py`](#ai-scheduler--optimizersai_schedulerpy)
  - [Flink Orchestrator — `flink_job/process_stream.py`](#flink-orchestrator--flink_jobprocess_streampy)
  - [Car Producer — `producers/produce_car_data.py`](#car-producer--producersproduce_car_datapy)
  - [Energy Producer — `producers/produce_energy_data.py`](#energy-producer--producersproduce_energy_datapy)
  - [Dashboard — `dashboard/dashboard.py`](#dashboard--dashboarddashboardpy)
- [Parallel Fleet Architecture](#parallel-fleet-architecture)
- [Q-Learning Agent](#q-learning-agent)
- [Physical Travel Simulation](#physical-travel-simulation)
- [Cost Formula](#cost-formula)
- [Dynamic Pricing Algorithm](#dynamic-pricing-algorithm)
- [Routing Decision Logic](#routing-decision-logic)
- [Kafka Topics](#kafka-topics)
- [Flink State Model](#flink-state-model)
- [Dashboard Panels](#dashboard-panels)
- [Infrastructure](#infrastructure)

---

## Architecture Overview

```
┌─────────────────────┐     cars_real      ┌─────────────────────────────────┐
│  Car Producer       │ ──────────────────► │                                 │
│  (40 EVs, 2D grid)  │                     │   Apache Flink                  │
└─────────────────────┘                     │   EVFleetProcessor              │
                                            │                                 │
┌─────────────────────┐     energy_data     │   • Builds NetworkX graph/tick  │
│  Energy Producer    │ ──────────────────► │   • Routes cars to stations     │
│  (3-station pricing)│                     │   • Manages reservations        │
└─────────────────────┘                     │   • Enforces per-station caps   │
                                            └──────────────┬──────────────────┘
                                                           │ charging_commands
                                                           ▼
                                            ┌─────────────────────────────────┐
                                            │  Streamlit Dashboard            │
                                            │  • 3-station price feed         │
                                            │  • Fleet grid map               │
                                            │  • Routing decision matrix      │
                                            │  • Reservation board            │
                                            └─────────────────────────────────┘
```

**Tick rate:** 1.25 seconds = 1 simulated 15-minute slot  
**Fleet:** 80 cars (40 heuristic + 40 AI), 3 stations, 96 slots/day  
**State storage:** Flink MapState (distributed, fault-tolerant)

---

## Quick Start

### Prerequisites
- Docker Desktop
- Python 3.9+
- `pip3 install kafka-python-ng streamlit plotly pandas numpy networkx`

### Start everything
```bash
bash deploy.sh
```

This single command:
1. Builds the custom ARM64 PyFlink Docker image
2. Starts Zookeeper, Kafka, Flink JobManager and TaskManager
3. Resets all Kafka topics (clean state)
4. Cancels any previous Flink jobs
5. Submits the new Flink job
6. Starts the Car Producer and Energy Producer
7. Starts the Streamlit dashboard at **http://localhost:8501**

### Stop everything
```bash
pkill -f produce_
pkill -f streamlit
docker-compose -f docker/docker-compose.yml down
```

---

## Project Structure

```
.
├── config/
│   ├── __init__.py
│   └── stations.py               # Single source of truth for stations + helpers
├── optimizers/
│   ├── __init__.py
│   ├── fleet_graph.py            # NetworkX knowledge graph (no Flink imports)
│   ├── reservation_scheduler.py  # Slot selection + replan logic
│   ├── heuristic_scheduler.py    # Fallback opportunistic charger logic
│   └── ai_scheduler.py           # Q-Learning agent for AI group routing
├── producers/
│   ├── produce_car_data.py       # Simulates 80 EV telemetry streams (40 pairs)
│   └── produce_energy_data.py    # Per-station dynamic pricing publisher
├── flink_job/
│   └── process_stream.py         # Main Flink KeyedProcessFunction
├── dashboard/
│   └── dashboard.py              # Streamlit real-time dashboard (3-view)
├── docker/
│   ├── Dockerfile                # Custom ARM64 PyFlink image
│   ├── docker-compose.yml        # Zookeeper + Kafka + Flink cluster
│   └── requirements.txt
├── deploy.sh                     # One-command full deploy
└── ev_logger.py                  # Structured logging across all components
```

---

## System Components

---

### Configuration — `config/stations.py`

Single source of truth for all physical constants and station definitions. Every other module imports from here instead of hardcoding values.

#### Constants

| Constant | Value | Description |
|---|---|---|
| `MAX_POWER_PER_CHARGER` | `11.0 kW` | Power per charger |
| `ENERGY_PER_SLOT` | `2.75 kWh` | Energy per 15-min slot (11 kW × 0.25 h) |
| `MOVE_STEP` | `0.1 km` | Car random-walk movement per tick |
| `TRAVEL_STEP_KM` | `0.2 km` | Directed travel speed toward a station per tick |
| `ARRIVE_THRESHOLD_KM` | `0.3 km` | Distance at which a car is considered "at station" |
| `CONSUMPTION_KWH_PER_KM` | `0.2 kWh/km` | EV drive consumption |
| `ROUND_TRIP_FACTOR` | `2` | Multiply one-way distance for round trip |

#### Station Definitions

| ID | Name | Position | Base Price | Capacity |
|---|---|---|---|---|
| `station_A` | Alpha | (2, 2) km | 50 €/MWh | 5 chargers |
| `station_B` | Beta | (8, 8) km | 40 €/MWh | 3 chargers |
| `station_C` | Gamma | (5, 2) km | 70 €/MWh | 4 chargers |

#### Helper Functions

```python
station_distance_km(car_x, car_y, station_id) -> float
```
Euclidean distance in km from a car's current grid position to a station.

```python
travel_cost_eur(car_x, car_y, station_id) -> float
```
Round-trip energy cost in EUR: `2 × distance × 0.2 kWh/km × base_price / 1000`.

```python
projected_price_for_station(reservation_count, station_id) -> float
```
State-based projected slot price given existing reservation count at a station.

---

### Knowledge Graph — `optimizers/fleet_graph.py`

An ephemeral `nx.DiGraph` rebuilt every Flink tick. Never persisted — lives only for the duration of one scheduling cycle (< 2 ms). Contains no Flink or Kafka imports; pure Python + NetworkX.

#### Graph Topology

```
car_{id}  ──[travel_cost_eur, distance_km]──►  station_A/B/C
station_A ──[projected_price]────────────────►  station_A:slot:42
car_{id}  ──[relation='reserved']────────────►  station_A:slot:42
```

#### Node Attributes

| Node Type | Key Attributes |
|---|---|
| `car` | `x`, `y`, `soc`, `emergency` |
| `station` | `x`, `y`, `base_price`, `capacity`, `active_chargers` |
| `slot` | `station_id`, `slot_idx`, `reservation_count`, `projected_price`, `is_full` |

#### Scale
~331 nodes, ~608 edges at full fleet size. Build time < 2 ms.

#### Functions

```python
build_fleet_graph(cars, station_reservation_counts, current_idx,
                  car_reservations=None) -> nx.DiGraph
```
Builds the full planning graph for one tick. `cars` must contain `x`, `y`, `current_soc` per car. `car_reservations` adds RESERVED edges for Phase 2 execution lookup.

```python
select_station_and_slots(G, car_id, num_needed, current_idx) -> (str | None, list[int])
```
**Normal car routing.** Iterates all car→station edges, for each station gathers available future slots, sorts by projected price, computes `travel_cost + charging_cost`, returns the globally cheapest `(station_id, slot_list)`.

```python
nearest_available_station(G, car_id, current_idx) -> str | None
```
**Emergency car routing.** Returns the station with minimum `distance_km` that has at least one non-full slot at `current_idx`. Price is ignored — speed is the only objective.

```python
get_cars_reserved_at_slot(G, slot_idx) -> dict[str, str]
```
Returns `{car_id: station_id}` for every car with a RESERVED edge at `slot_idx`. Used in Phase 2 to build the charger assignment lookup.

```python
get_reservation_summary(G) -> dict[str, dict[int, int]]
```
Extracts `{station_id: {slot_idx: count}}` from slot node attributes. Used to build the `RESERVATION_STATUS` Kafka message.

```python
snapshot_graph(G) -> str
```
Serialises the graph to JSON (NetworkX node-link format). Debug/academic use only — not called in the hot path. Reconstruct with `nx.node_link_graph(json.loads(snapshot))`.

---

### Reservation Scheduler — `optimizers/reservation_scheduler.py`

Pure planning functions used by Flink for slot selection and replan decisions. No Flink or Kafka imports.

#### Functions

```python
target_soc(current_soc) -> float
```
Returns the charging target SOC:
- SOC < 15% → emergency, charge to **50%**
- SOC 15–50% → needs charge, fill to **100%**
- SOC > 50% → returns `-1` (no charge needed)

```python
slots_needed(current_soc) -> int
```
Calculates how many 15-min charging slots are needed to reach the target. Applies a **15% energy buffer** for non-emergency cars to compensate for battery discharge between spread-out charging slots.

```python
projected_price(reservation_count) -> float
```
State-based projected slot price based on how many reservations already exist (off-peak / normal / peak multiplier against `BASE_PRICE`).

```python
should_replan(car_reservation, current_idx, current_soc) -> bool
```
Returns `True` (replan triggered) when any of these conditions hold:
- No existing reservations
- SOC dropped ≥ 10% since last plan
- ≥ 16 slots (4 simulated hours) have elapsed since last plan
- All reserved slots are now in the past

```python
select_cheapest_slots(current_idx, num_needed, reservation_counts,
                      is_emergency=False) -> list[int]
```
Selects optimal future slots. Emergency cars receive consecutive slots starting at `current_idx`. Normal cars receive the globally cheapest slots spread across the remainder of the day, skipping any full slots.

---

### Flink Orchestrator — `flink_job/process_stream.py`

The core stream processor. Implements `KeyedProcessFunction` keyed on the constant `"fleet"` so all car and energy data lands in a single stateful operator with access to all cars simultaneously.

#### Processing Phases (fired every 1250 ms via processing-time timer)

**Phase 1 — Planning**
1. Load all cars and existing reservations from MapState
2. Sort cars by SOC ascending (most critical plan first — gets first pick of slots)
3. For each car where `should_replan()` returns `True`:
   - Remove existing reservations from the board
   - Rebuild the fleet graph from current MapState
   - Emergency (SOC < 15%): call `nearest_available_station()`
   - Normal car: call `select_station_and_slots()` (minimises total EUR)
   - Write new reservations to `reservation_board` with key `"station_id:slot_idx"`
   - **Graph is rebuilt after each car's commit** so subsequent cars see accurate counts

**Phase 2 — Execution**
1. Rebuild graph one final time with the complete reservation state
2. Identify which cars have reservations for the current slot index
3. Assign physical chargers per station (capacity enforced independently):
   - **Priority 1:** Emergency cars (SOC < 15%)
   - **Priority 2:** Recovering cars (SOC < 50%, previously emergency)
   - **Priority 3:** Cars with a reservation at this slot
   - **Priority 4:** Opportunistic / heuristic fallback
4. Emit `START_CHARGING` / `STOP_CHARGING` with `station_id` and `reserved_slots`
5. Emit `RESERVATION_STATUS` with per-station reservation counts and projected prices

#### Stale Slot Cleanup
Every tick, `slot_idx - 1` is removed from the board across all stations to prevent past reservations from inflating future projected prices on the dashboard.

#### Flink State

| State | Type | Key | Description |
|---|---|---|---|
| `cars` | MapState | `car_id` | Latest telemetry per car |
| `station_prices` | MapState | `station_id` | Current live price per station |
| `reservation_board` | MapState | `"station_A:47"` | Car IDs reserved per station+slot |
| `car_reservations` | MapState | `car_id` | Full reservation plan per car |
| `was_charging` | MapState | `car_id` | Previous tick charging state (for STOP detection) |
| `interval_idx` | ValueState | — | Current simulation slot (0–95) |
| `timer_set` | ValueState | — | Whether a processing-time timer is pending |
| `day_of_week` | ValueState | — | Simulated day (0=Mon … 6=Sun) |

---

### AI Scheduler — `optimizers/ai_scheduler.py`

Q-Learning agent that replaces NetworkX graph routing for the `ai` group. Instantiated once per Flink job and lives in memory for the full run — no persistence across restarts (the learning curve is part of the academic comparison story).

#### State Space — 13-tuple

```
(soc_band,
 price_A_level, occ_A_level, dist_A_band, trend_A,
 price_B_level, occ_B_level, dist_B_band, trend_B,
 price_C_level, occ_C_level, dist_C_band, trend_C)
```

| Dimension | Values | Source |
|---|---|---|
| `soc_band` | 0=low(<30%), 1=mid(<50%), 2=high(≥50%) | car state |
| `price_X_level` | 0=off-peak, 1=normal, 2=peak | live station price vs base |
| `occ_X_level` | 0=free, 1=half-full, 2=full | reservation board counts |
| `dist_X_band` | 0=close(<3km), 1=medium(<7km), 2=far(≥7km) | NetworkX graph edge |
| `trend_X` | 0=falling, 1=stable, 2=rising | next-2-slot occupancy vs current |

**Total states:** 3¹³ = 1,594,323 — stored as a Python dict (only visited states allocated).

#### Hyperparameters

| Parameter | Value | Description |
|---|---|---|
| `ALPHA` | 0.15 | Learning rate |
| `GAMMA` | 0.85 | Discount factor |
| `EPSILON_START` | 0.80 | Initial exploration probability |
| `EPSILON_MIN` | 0.10 | Floor — never fully stop exploring |
| `EPSILON_DECAY` | 0.997 | Multiplied after every Q-update |

#### Actions
`['station_A', 'station_B', 'station_C', None]`  
`None` = don't book this tick (used when all stations are too expensive).

#### Reward Function
```
reward = (2.0 − price/base_price) × num_slots   # cheaper = higher reward
       − travel_eur × 2.0                         # penalise long trips
       + urgency_bonus                             # +3.0 if SOC<30%, +1.0 if SOC<50%

no-book penalty: −5.0 if SOC < 30%, −1.0 otherwise
```

#### Key difference vs Heuristic
The heuristic has a **complete global view** — it evaluates all stations simultaneously and picks the mathematical minimum. The Q-agent learns through **trial and error** over many ticks. During early training (ε≈0.80), random exploration consumes cheap slots (exploration tax). As ε decays to 0.10, decisions improve but the slot pool may already be depleted — this is a known tradeoff of tabular Q-learning in finite resource environments and is visible in the Comparison tab.

---

### Car Producer — `producers/produce_car_data.py`

Simulates 80 autonomous EVs (40 identical pairs) publishing telemetry to Kafka every 1.25 seconds.

#### Paired Fleet

```python
NUM_PAIRS = 40   # 40 heuristic (h_car_1…40) + 40 AI (a_car_1…40) = 80 total
```

Each pair `(h_car_i, a_car_i)` is created with **identical** starting conditions:
- Same `start_battery` (random 10–100%)
- Same discharge constant `k` (random 0.4–1.0)
- Same starting `x`, `y` position

This guarantees a **fair comparison** — any performance difference between groups is due purely to the routing algorithm.

#### `Car` Class

```python
Car(car_id, group, start_battery, priority, k, x, y)
```

| Attribute | Description |
|---|---|
| `battery_level` | Current SOC as percentage (0–100) |
| `x`, `y` | 2D grid position in km, range 0–10 |
| `target_x`, `target_y` | Station coordinates to travel toward (None = roam freely) |
| `is_charging` | Controlled by Flink `START_CHARGING` commands |
| `priority` | Integer priority (1–5) |
| `k` | Discharge rate coefficient per tick |

**`Car.update(demand_mult)`**
- If `is_charging`: position frozen at station coordinates
- If `target_x` set (in transit): moves `TRAVEL_STEP_KM` directly toward target each tick; snaps to station and clears target when within `ARRIVE_THRESHOLD_KM`
- Otherwise: random walk ±`MOVE_STEP` per axis; battery discharges by `k × demand_mult`

**`Car.to_dict()`**
Serialises to Kafka payload: `id`, `group`, `current_soc`, `current_battery_kwh`, `priority`, `plugged_in`, `x`, `y`, `in_transit`, `target_x`, `target_y`, `timestamp`.

#### `demand_multiplier(interval_idx, day_of_week) -> float`
Simulates real-world EV charging demand patterns:

| Time Window | Factor | Reason |
|---|---|---|
| Weekday 16:00–20:00 | ×2.5 | Commuter peak |
| All days 00:00–08:00 | ×0.3 | Overnight, low activity |
| Friday evening / Saturday | ×0.4 | Leisure, no commute |
| Default | ×1.0 | Normal daytime |

---

### Energy Producer — `producers/produce_energy_data.py`

Computes and publishes independent dynamic prices for all 3 stations every 1.25 seconds. Runs a background thread that consumes `charging_commands` to track active charger counts per station.

#### Two-Component Pricing Algorithm (independent per station)

**Component 1 — Instantaneous (60% weight, reactive)**
Maps current charger occupancy to a price multiplier:

| Occupancy | Multiplier | State |
|---|---|---|
| ≤ 30% | ×0.6 | Off-peak |
| ≤ 60% | ×1.0 | Normal |
| > 60% | ×1.8 | Peak |

**Component 2 — Learned Hourly Profile (40% weight, predictive)**
Builds a histogram of peak-hour observations per station. Once ≥ 4 observations exist for a given hour, raises the price proactively before traffic arrives — even when chargers are currently idle.

**Compound Boost**
When both components are simultaneously in peak state: final price is multiplied by **×1.3**.  
Maximum possible price: `base_price × 1.8 × 1.3`.

**Blending formula:**
```
final = (instant_price × 0.6 + hist_price × 0.4) × compound_factor
```

#### Functions

```python
compute_station_price(active, hour, capacity, base_price, station_id) -> dict
```
Returns full breakdown: `current_price`, `instant_price`, `hist_price`, `peak_freq`, `compound_boost`, `price_state`, `active_chargers`, `hourly_profile` (per-station learned histogram).

```python
_get_multiplier(active, capacity) -> float
```
Maps occupancy ratio to price multiplier via `PRICE_LEVELS`.

```python
_update_hourly_profile(station_id, hour, is_peak)
```
Increments the learning histogram `{total_count, peak_count}` for the given station and hour.

```python
_snapshot_hourly_profile() -> dict
```
Aggregates hourly profiles across all stations for backward-compatible aggregate fields.

```python
consume_commands()
```
Background thread. Consumes `charging_commands`, tracks `station_active_car_ids` per station from `START_CHARGING` / `STOP_CHARGING` actions, and receives `RESERVATION_STATUS` updates from Flink.

---

### Dashboard — `dashboard/dashboard.py`

Streamlit single-page application that polls all 3 Kafka topics directly and auto-refreshes every second via `time.sleep(1); st.rerun()`.

#### Session State Keys

| Key | Type | Description |
|---|---|---|
| `cars` | `dict` | `car_id → latest telemetry dict` |
| `car_decisions` | `dict` | `car_id → CHARGING / IDLE / DEFERRED_PRICE / DEFERRED_FULL / EMERGENCY` |
| `car_stations` | `dict` | `car_id → station_id` (last known assignment) |
| `car_reserved_slots` | `dict` | `car_id → list[int]` reserved slot indices |
| `station_data` | `dict` | `station_id → {current_price, active_chargers, price_state, ...}` |
| `station_price_histories` | `dict` | `station_id → list[float]` rolling 120-tick price history |
| `station_charger_histories` | `dict` | `station_id → list[int]` rolling active charger count history |
| `station_hourly_profiles` | `dict` | `station_id → {hour: {peak_count, total_count}}` |
| `per_station_reservation_counts` | `dict` | `station_id → {slot_idx: count}` |
| `per_station_projected_prices` | `dict` | `station_id → list[float]` 96-slot projected price curve |
| `projected_prices` | `list` | Aggregate 96-slot projected price curve |
| `dynamic_prices` | `list` | Rolling aggregate price history (120 ticks) |
| `time_labels` | `list` | HH:MM strings aligned to `dynamic_prices` |

#### Helper Functions

```python
soc_color(soc) -> str
```
Returns hex colour for a SOC value: `#ff4b4b` red (< 15%), `#fed330` yellow (< 40%), `#26de81` green (< 75%), `#00e5ff` cyan (≥ 75%).

```python
occupancy_color(n, total) -> str
```
Returns hex colour for charger occupancy: cyan (0%), green (≤ 40%), yellow (≤ 80%), red (> 80%).

```python
poll_kafka()
```
Polls all 3 Kafka topics with 100 ms timeout (3000 ms on first connect). Processes:
- `DYNAMIC_PRICE` → updates `station_data`, `station_price_histories`, `station_charger_histories`, `station_hourly_profiles`
- Car telemetry → updates `cars` dict
- `START_CHARGING` / `STOP_CHARGING` → updates `car_decisions`, `car_stations`, `car_reserved_slots`
- `RESERVATION_STATUS` → updates `per_station_reservation_counts`, `per_station_projected_prices`

---

## Parallel Fleet Architecture

The system runs two groups of 40 cars simultaneously in the **same Flink job**, competing for the same physical chargers.

| Group | Car IDs | Routing | Phase 1 Decision |
|---|---|---|---|
| Heuristic | `h_car_1` … `h_car_40` | NetworkX graph | `select_station_and_slots()` — global minimum of travel + charging cost |
| AI | `a_car_1` … `a_car_40` | Q-Learning agent | `QLearningAgent.select_action()` — learned policy from Q-table |

Both groups share the same reservation board, the same physical chargers, and the same dynamic prices. Phase 2 execution (charger allocation, capacity enforcement) is identical for both groups.

**Per-group metrics** are emitted every tick in `RESERVATION_STATUS.group_metrics`:
- `charge_count`, `emergency_count`, `avg_cost_eur`, `avg_soc`, `car_count`

**Dashboard Comparison tab** displays these metrics side by side with a bar chart.

---

## Q-Learning Agent

See [`optimizers/ai_scheduler.py`](#ai-scheduler--optimizersai_schedulerpy) for full detail.

#### Why the heuristic can still win

During early training, the Q-agent's ε=0.80 means 80% of decisions are random. Random choices consume cheap/off-peak slots that the heuristic would have used. By the time ε decays and the agent has learned, the best slots are already taken — the agent is forced into leftovers. This **exploration tax** is a fundamental tradeoff of tabular Q-learning in finite-resource environments and is clearly visible in the Comparison tab over time.

#### What the Q-agent can do that the heuristic cannot

- Encodes **distance** (via NetworkX edge data) into its state — learns "station B cheap AND close → prefer it"
- Encodes **price trend** — rising occupancy in next 2 slots means "book now before it peaks"
- Accumulates experience across all 40 AI cars (shared Q-table instance within the Flink job)

---

## Physical Travel Simulation

Cars physically travel to their assigned station before they can charge.

#### Flow
1. **Phase 1 (reservation):** Flink picks a station → emits `SET_TARGET` command with station `x`, `y`
2. **Producer:** Car moves `TRAVEL_STEP_KM = 0.2 km/tick` directly toward target instead of random walking; battery still drains during travel
3. **Arrival:** When within `ARRIVE_THRESHOLD_KM = 0.3 km`, car snaps to station coordinates and clears target
4. **Phase 2 (execution):** Flink checks `distance(car, station) ≤ ARRIVE_THRESHOLD_KM` before allowing charging — cars still in transit receive `STOP_CHARGING` with `reason=in_transit`
5. **Charging start:** `START_CHARGING` command snaps car to exact station coordinates; car freezes there until charging stops

#### Travel time examples

| Distance | Ticks to arrive | Real time |
|---|---|---|
| 2 km | 10 ticks | ~12.5 s |
| 5 km | 25 ticks | ~31 s |
| 10 km | 50 ticks | ~62 s |

#### Dashboard visibility
- **Dashed yellow lines** on Fleet Grid Map: car → target station (in transit)
- **Solid cyan lines**: car → station (plugged in / charging)
- `TRANSIT` badge in Fleet Status cards
- Car hover tooltip shows `[TRANSIT]`

---

## Cost Formula

The fundamental optimisation every non-emergency car solves each replan:

```
distance_km  = sqrt((car_x − stn_x)² + (car_y − stn_y)²)

travel_EUR   = 2 × distance_km × 0.2 kWh/km × station.base_price / 1000
                                                └─ EUR/MWh ÷ 1000 = EUR/kWh

charging_EUR = num_slots × 2.75 kWh × live_price_at_station / 1000

total_EUR    = travel_EUR + charging_EUR   ← minimised by select_station_and_slots()
```

**Example at max grid distance (~14 km), base price 50 €/MWh:**
- Travel cost ≈ **0.28 €**
- Charging cost for 10 slots at off-peak ≈ **1.43 €**
- Travel is ~17% of total → a real factor in the decision, not negligible

---

## Dynamic Pricing Algorithm

```
off_peak_price  = base_price × 0.6
normal_price    = base_price × 1.0
peak_price      = base_price × 1.8
compound_peak   = peak_price × 1.3   (both components peak simultaneously)

instant_mult  = PRICE_LEVELS lookup on (active_chargers / capacity)

hist_freq     = peak_count[hour] / total_count[hour]   (requires ≥ 4 observations)
hist_mult     = off_mult + (peak_mult − off_mult) × hist_freq

blended_price = base_price × instant_mult × 0.6
              + base_price × hist_mult    × 0.4

final_price   = blended_price × (1.3 if both_peak else 1.0)
```

Each station learns independently. A station that is consistently busy at 18:00 will raise its price at 17:45 — before cars arrive — via the predictive 40% component.

---

## Routing Decision Logic

```
for each car sorted by SOC ascending (most critical first):
    if should_replan():
        remove old reservations from board

        if emergency (SOC < 15%):
            station = nearest_available_station(G)   # minimum distance, ignores price
            slots   = consecutive slots from current_idx

        else (normal car):
            for each station:
                travel_cost  = 2 × dist × 0.2 × base_price / 1000
                candidates   = future non-full slots sorted by projected_price
                charging_eur = Σ (slot_price × 2.75 / 1000) for cheapest N slots
                total        = travel_cost + charging_eur

            station, slots = argmin(total_eur across all stations)

        write slots to reservation_board["station_id:slot_idx"]
        rebuild graph → next car sees updated reservation counts
```

---

## Kafka Topics

| Topic | Producer | Consumers | Message Types |
|---|---|---|---|
| `cars_real` | Car Producer | Flink | Car telemetry: `id`, `group`, `current_soc`, `x`, `y`, `in_transit`, `target_x`, `target_y`, `plugged_in`, `priority`, `timestamp` |
| `energy_data` | Energy Producer | Flink, Dashboard | `DYNAMIC_PRICE` with per-station pricing breakdown and learned hourly profiles |
| `charging_commands` | Flink | Car Producer, Energy Producer, Dashboard | `START_CHARGING`, `STOP_CHARGING`, `SET_TARGET`, `RESERVATION_STATUS` (with `group_metrics`, `q_agent_stats`) |

---

## Flink State Model

```
EVFleetProcessor  (KeyedProcessFunction, key = "fleet")
│
├── MapState  cars                    car_id  →  JSON telemetry string
├── MapState  station_prices          station_id  →  float (€/MWh)
├── MapState  reservation_board       "station_A:47"  →  JSON list of car_ids
├── MapState  car_reservations        car_id  →  {station_id, reserved_slots,
│                                                 planned_soc, planned_at_idx}
├── MapState  was_charging            car_id  →  bool
├── ValueState  interval_idx          int  (current slot 0–95)
├── ValueState  timer_set             bool  (processing-time timer pending)
└── ValueState  day_of_week           int  (0=Mon … 6=Sun)
```

---

## Dashboard Panels

The dashboard uses `st.radio(horizontal=True)` navigation (not `st.tabs`) so only the selected view's Python code executes — preventing ghost content bleed between views.

#### Navigation Views

| View | Contents |
|---|---|
| **Heuristic** | Full dashboard filtered to `h_car_*` group only |
| **AI Agent** | Full dashboard filtered to `a_car_*` group + Q-agent epsilon/reward sparkline |
| **Comparison** | Metrics table + bar chart comparing both groups — no charts, no panels |

#### Panels (Heuristic and AI Agent views)

| Panel | Column | Description |
|---|---|---|
| Diagnostics bar | Full width | Kafka status, message counts per topic, host |
| Station cards (×3) | Full width | Live price, charger occupancy bar, price state per station |
| Real-Time Price Feed | Left | 3 coloured lines — rolling price history per station |
| Active Chargers | Left | 3 lines — busy charger count per station vs. capacity cap |
| Peak Frequency by Hour | Left | 3 tabs — per-station learned peak-hour histogram |
| Willingness-to-Pay | Left | SOC band → max price a car in that band will accept |
| Reservation Board | Left | Per-station slot grid with charger capacity (5 / 3 / 4) |
| Fleet Grid Map | Left | 2D scatter: car dots coloured by SOC, station squares by occupancy; **dashed yellow lines** = cars in transit; solid cyan lines = cars charging |
| Routing Decision Matrix | Right | Per-car: Travel€ + Charge€ = Total€ for all 3 stations; ✓ marks the current cheapest option using occupancy-projected prices (matches Flink logic) |
| Charging Stations | Right | Live charger slots per station — which car is currently plugged in |
| Fleet Status | Right | Compact car cards: SOC bar, kWh, priority, station badge; badges include `CHARGING`, `TRANSIT`, `EMERGENCY`, `DEFERRED_PRICE`, `DEFERRED_FULL`, `IDLE` |

---

## Infrastructure

### Docker Services

| Service | Image | Port | Role |
|---|---|---|---|
| `zookeeper` | `confluentinc/cp-zookeeper:7.5.0` | 2181 | Kafka coordination |
| `kafka` | `confluentinc/cp-kafka:7.5.0` | 9092 | Message broker (single-broker dev setup) |
| `flink-jobmanager` | `custom-pyflink-arm64:1.17.0` | 8081 | Job submission, Flink Web UI |
| `flink-taskmanager` | `custom-pyflink-arm64:1.17.0` | — | Task execution (2 task slots) |

### Custom PyFlink Docker Image
Built from `docker/Dockerfile` on top of `flink:1.17.0-java11` (ARM64):
- Python 3.9, pip, gcc build tools
- Python packages: `kafka-python-ng`, `numpy`, `pulp`, `pandas`, `streamlit`, `networkx`, `apache-flink==1.17.0`
- Kafka SQL connector JAR: `flink-sql-connector-kafka-1.17.0.jar`

### Flink Web UI
Available at **http://localhost:8081** while the system is running.  
Shows job graph, task throughput, backpressure indicators, and TaskManager metrics.
