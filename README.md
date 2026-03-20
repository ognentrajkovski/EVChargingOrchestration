# Real-Time Cost Optimization Engine for Autonomous EV Fleets

A streaming data pipeline that dynamically prices EV charging slots based on real-time fleet demand and learned historical patterns, then orchestrates a fleet of 40 autonomous vehicles to charge at the cheapest possible times.

---

## Architecture

```
┌─────────────────┐     cars_real      ┌──────────────────────┐
│  Car Producer   │ ─────────────────► │                      │
│  (40 EVs)       │                    │   Apache Flink       │
└─────────────────┘                    │   (process_stream.py)│
                                       │                      │   charging_commands
┌─────────────────┐     energy_data    │  Phase 1 — Planning  │ ──────────────────►  ┌───────────────┐
│ Energy Producer │ ─────────────────► │  Phase 2 — Execution │                      │   Dashboard   │
│ (Dynamic Price) │ ◄──────────────── │                      │                      │  (Streamlit)  │
└─────────────────┘  charging_commands └──────────────────────┘                      └───────────────┘
```

**Kafka topics:**
| Topic | Producer | Consumers |
|---|---|---|
| `energy_data` | Energy Producer | Flink, Dashboard |
| `cars_real` | Car Producer | Flink, Dashboard |
| `charging_commands` | Flink | Car Producer, Energy Producer, Dashboard |

---

## Components

### Car Producer — `producers/produce_car_data.py`
- Simulates **40 EVs** with randomised SOC, priority, and discharge rate
- Battery drains at a rate that varies by time of day and day of week (e.g. ×2.5 during weekday evening rush, ×0.3 overnight)
- Listens to Flink's `charging_commands` to apply SOC updates when a car is told to start/stop charging
- Publishes car state to `cars_real` every 1.25 s

### Energy Producer — `producers/produce_energy_data.py`
Computes a **two-component dynamic price** every tick:

```
Final price = (instantaneous_price × 0.60) + (historical_price × 0.40)
```

| Component | Weight | Logic |
|---|---|---|
| Instantaneous | 60% | Counts active chargers → maps to price state (off-peak / normal / peak) |
| Hourly profile | 40% | Tracks how often each hour 0–23 was historically in peak state; nudges price up pre-emptively before rush hours |

**Price states:**
| State | Occupancy | Multiplier | Price |
|---|---|---|---|
| Off-peak | 0–30% | ×0.6 | 30 €/MWh |
| Normal | 31–60% | ×1.0 | 50 €/MWh |
| Peak | 61–100% | ×1.8 | 90 €/MWh |

**Compound boost ×1.3** — applied when both the instantaneous state AND the historical profile are simultaneously in peak, making it more expensive to charge during historically busy hours that are also currently busy.

### Flink Orchestrator — `flink_job/process_stream.py`
Runs a **KeyedProcessFunction** with a 1.25 s simulation tick. Each tick has two phases:

**Phase 1 — Planning:**
- Each car (sorted lowest SOC first) reserves the cheapest future 15-min slots for the day
- Greedy slot selection using projected prices that update as more cars reserve slots (earlier cars get first pick of cheap slots)
- Reservations are replanned when: SOC drops 10%+, every 16 slots (~4 h), or all reserved slots are in the past

**Phase 2 — Execution:**
Priority queue for the 5 physical chargers:
1. **Emergency** (SOC < 15%) — always charge
2. **Reserved** — cars with a reservation for the current slot
3. **Opportunistic** — heuristic fallback based on SOC band and current price
4. **Test cars** — naive always-charge logic (for comparison)

After execution, emits a `RESERVATION_STATUS` message so the dashboard and energy producer can show projected prices.

### Schedulers — `optimizers/`

**`heuristic_scheduler.py`** — Real-time per-tick bidding decision:
- Emergency SOC (< 15%) → charge at any price
- SOC 15–30% → pay up to ×2.5 base price
- SOC 30–50% → pay up to ×1.8 base price
- SOC ≥ 50% → don't charge opportunistically
- Hysteresis bonus +0.6× if already holding a charger slot (prevents ping-pong)

**`reservation_scheduler.py`** — Day-ahead slot planning:
- `slots_needed()` — calculates how many 15-min slots a car needs to reach its target SOC
- `select_cheapest_slots()` — picks cheapest future slots, considering existing reservations
- `projected_price()` — state-based price projection matching the energy producer formula

### Dashboard — `dashboard/dashboard.py`
Real-time Streamlit dashboard (auto-refreshes every 1 s):

- **State Badge** — current price state (OFF-PEAK / NORMAL / PEAK) with instant vs. historical price breakdown and COMPOUND ×1.3 alert
- **Real-Time Price Feed** — sparkline with green/yellow/red colored segments by price zone
- **Active Charger History** — rolling count of chargers in use
- **Price States Ladder** — 3 state rows with active state highlighted
- **What the Model Learned** — bar chart of peak frequency per hour 0–23 (shows what the pricing model has learned)
- **Daily Price Profile** — projected prices for the full day + past period snapshots
- **Traffic → Price Impact** — bar chart showing price at each occupancy level
- **Charging Stations** — 5 physical charger slots, synchronized with Reservation Board
- **Reservation Board** — shows which car is assigned to which charger and for which time window
- **Fleet Status** — per-car SOC bar, kWh, priority, and decision badge (CHARGING / EMERGENCY / DEFERRED / IDLE)

---

## Getting Started

### Prerequisites
- Docker & Docker Compose
- Python 3.10+
- `pip install kafka-python streamlit plotly pandas numpy`

### Run Everything (one command)

```bash
./deploy.sh
```

This will:
1. Build and start Kafka + Zookeeper + Flink (JobManager + TaskManager) via Docker Compose
2. Reset all Kafka topics
3. Cancel any previous Flink jobs
4. Submit the Flink streaming job
5. Start the car producer and energy producer as background processes
6. Start the Streamlit dashboard

**Dashboard:** http://localhost:8501
**Flink UI:** http://localhost:8081

### Stop

```bash
# Stop producers and dashboard
pkill -f produce_
pkill -f streamlit

# Stop Docker infrastructure
docker-compose -f docker/docker-compose.yml down
```

### Run Components Individually

```bash
# Start infrastructure only
docker-compose -f docker/docker-compose.yml up -d

# Submit Flink job
docker-compose -f docker/docker-compose.yml exec flink-jobmanager \
  flink run -d -pyfs /opt/flink/project/optimizers \
  -py /opt/flink/project/flink_job/process_stream.py

# Start producers
export KAFKA_BOOTSTRAP_SERVERS=localhost:9092
python producers/produce_car_data.py &
python producers/produce_energy_data.py &

# Start dashboard
streamlit run dashboard/dashboard.py
```

---

## Project Structure

```
.
├── deploy.sh                        # One-command startup script
├── docker/
│   ├── docker-compose.yml           # Kafka + Zookeeper + Flink services
│   ├── Dockerfile                   # Custom PyFlink image (ARM64)
│   └── requirements.txt
├── flink_job/
│   └── process_stream.py            # Flink orchestrator (planning + execution)
├── optimizers/
│   ├── heuristic_scheduler.py       # Real-time bidding decision logic
│   ├── reservation_scheduler.py     # Day-ahead slot reservation planner
│   └── lp_scheduler.py              # LP-based scheduler (alternative)
├── producers/
│   ├── produce_car_data.py          # 40-car fleet simulator
│   └── produce_energy_data.py       # Dynamic pricing algorithm
├── dashboard/
│   └── dashboard.py                 # Streamlit real-time dashboard
└── ev_logger.py                     # Shared structured logger
```

---

## Key Design Decisions

**Why Kafka + Flink?**
The system needs to react to price changes and SOC updates in under 2 seconds across 40 cars simultaneously. Kafka decouples the producers so any component can be restarted independently. Flink's stateful KeyedProcessFunction keeps car state, reservations, and the charging board in memory without a database.

**Why two-component pricing?**
Pure instantaneous pricing reacts too late — by the time 4 cars plug in, the price is already peak. The 40% historical component lets the system raise prices pre-emptively during hours that are historically busy, smoothing demand before the rush arrives.

**Why reservation-based scheduling?**
Without reservations, all cars try to charge at the same cheap time slots, immediately making those slots expensive. With reservations, earlier-planned cars see updated projected prices so subsequent cars are naturally distributed across cheaper slots — a simple coordination mechanism without any central auction.

**Compound boost**
When the current hour is both instantaneously at peak utilisation AND historically a peak hour, an extra ×1.3 multiplier is applied. This creates a stronger price signal specifically during the double-peak scenario (e.g. a Monday evening when the chargers are full and historically they always are at this time).
