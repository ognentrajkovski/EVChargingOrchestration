import os
import sys
import time
import json
import uuid
import math

import streamlit as st
import pandas as pd
import numpy as np
import plotly.graph_objects as go
import plotly.io as pio
# Force plotly to use the stdlib json engine instead of orjson.
# orjson can partially-initialise under Streamlit's multi-thread re-runs on
# Python 3.9, producing "partially initialized module 'orjson'" errors.
pio.json.config.default_engine = 'json'
from kafka import KafkaConsumer

# ── Logging ────────────────────────────────────────────────────────────────────
_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__))) if '__file__' in dir() else '.'
sys.path.insert(0, _root)
# Make fleet_graph importable (lives in optimizers/)
sys.path.insert(0, os.path.join(_root, 'optimizers'))
sys.path.insert(0, os.path.join(_root, 'config'))
try:
    from ev_logger import log
    _dash_log = lambda msg: log('DASH', msg)
except Exception:
    _dash_log = lambda msg: None

# ── Page config ────────────────────────────────────────────────────────────────
st.set_page_config(page_title="EV Fleet Control", layout="wide", page_icon="⚡")

# ── Global CSS ─────────────────────────────────────────────────────────────────
st.markdown("""
<style>
@import url('https://fonts.googleapis.com/css2?family=Inter:wght@300;400;500;600&family=JetBrains+Mono:wght@400;500&display=swap');

html, body, [class*="css"] {
    font-family: 'Inter', sans-serif;
    background-color: #060b14;
    color: #8ba3bc;
}
.stApp { background-color: #060b14; }

::-webkit-scrollbar { width: 4px; }
::-webkit-scrollbar-track { background: #0a1220; }
::-webkit-scrollbar-thumb { background: #1a2d44; border-radius: 2px; }

#MainMenu, footer, header { visibility: hidden; }
.block-container { padding-top: 1.5rem; padding-bottom: 1rem; }

/* ── Keyframes ── */
@keyframes pulse-border {
    0%   { box-shadow: 0 0 0 1px #00e5ff18, 0 4px 24px #00e5ff08; border-color: #00e5ff30; }
    50%  { box-shadow: 0 0 0 1px #00e5ff55, 0 4px 32px #00e5ff20; border-color: #00e5ff80; }
    100% { box-shadow: 0 0 0 1px #00e5ff18, 0 4px 24px #00e5ff08; border-color: #00e5ff30; }
}
@keyframes critical-pulse {
    0%, 100% { box-shadow: 0 0 0 1px #ff4b4b20; border-color: #ff4b4b30; }
    50%       { box-shadow: 0 0 0 1px #ff4b4b60; border-color: #ff4b4b70; }
}
@keyframes bolt-flash {
    0%, 100% { opacity: 1; }
    50%       { opacity: 0.5; }
}
@keyframes batt-shimmer {
    0%   { background-position: -200% center; }
    100% { background-position: 200% center; }
}
@keyframes scan-line {
    0%   { transform: translateY(-100%); opacity: 0; }
    15%  { opacity: 0.8; }
    85%  { opacity: 0.8; }
    100% { transform: translateY(500%); opacity: 0; }
}
@keyframes fade-in {
    from { opacity: 0; transform: translateY(4px); }
    to   { opacity: 1; transform: translateY(0); }
}
@keyframes deferred-pulse {
    0%, 100% { opacity: 1; }
    50%       { opacity: 0.5; }
}

/* ── Title ── */
.dash-title {
    font-family: 'JetBrains Mono', monospace;
    font-size: 1.1rem;
    font-weight: 500;
    letter-spacing: 0.2em;
    text-transform: uppercase;
    padding-bottom: 0.8rem;
    margin-bottom: 0.5rem;
    border-bottom: 1px solid #0f1f30;
    background: linear-gradient(90deg, #00e5ff, #0090ff);
    -webkit-background-clip: text;
    -webkit-text-fill-color: transparent;
}

/* ── Clock strip ── */
.clock-strip {
    display: flex;
    gap: 0;
    align-items: stretch;
    background: #080f1a;
    border: 1px solid #0f1f30;
    border-radius: 10px;
    overflow: hidden;
    margin-bottom: 1.2rem;
}
.clock-cell {
    flex: 1;
    padding: 0.7rem 1.2rem;
    border-right: 1px solid #0f1f30;
}
.clock-cell:last-child { border-right: none; }
.clock-val {
    font-family: 'JetBrains Mono', monospace;
    font-size: 1.6rem;
    font-weight: 500;
    color: #e2eaf4;
    line-height: 1.1;
    letter-spacing: 0.04em;
}
.clock-val.accent { color: #00e5ff; }
.clock-label {
    font-size: 0.62rem;
    font-weight: 500;
    color: #2d4a62;
    text-transform: uppercase;
    letter-spacing: 0.15em;
    margin-bottom: 0.2rem;
}

/* ── Section headers ── */
.section-hdr {
    font-family: 'JetBrains Mono', monospace;
    font-size: 0.65rem;
    font-weight: 500;
    letter-spacing: 0.22em;
    color: #2d4a62;
    text-transform: uppercase;
    margin: 1.2rem 0 0.6rem;
    display: flex;
    align-items: center;
    gap: 0.6rem;
}
.section-hdr::before {
    content: '';
    display: inline-block;
    width: 3px;
    height: 12px;
    background: linear-gradient(180deg, #00e5ff, #0050ff);
    border-radius: 2px;
    flex-shrink: 0;
}
.section-hdr::after {
    content: '';
    flex: 1;
    height: 1px;
    background: linear-gradient(90deg, #0f1f30, transparent);
}

/* ── Battery bar ── */
.batt-fill {
    height: 100%;
    border-radius: 3px;
    transition: width 0.8s cubic-bezier(0.4, 0, 0.2, 1);
}
.batt-fill.charging {
    background-size: 200% auto;
    animation: batt-shimmer 2s linear infinite;
}

/* ── Status badge ── */
.badge {
    display: inline-flex;
    align-items: center;
    gap: 0.3rem;
    font-size: 0.6rem;
    font-family: 'JetBrains Mono', monospace;
    font-weight: 500;
    letter-spacing: 0.1em;
    padding: 0.2rem 0.6rem;
    border-radius: 4px;
    text-transform: uppercase;
}
.badge-charging {
    background: linear-gradient(135deg, #00e5ff12, #0050ff08);
    color: #00e5ff;
    border: 1px solid #00e5ff25;
    animation: bolt-flash 1.5s ease-in-out infinite;
}
.badge-idle {
    background: #0a1525;
    color: #2d4a62;
    border: 1px solid #0f1f30;
}
.badge-critical {
    background: linear-gradient(135deg, #ff4b4b12, #ff000008);
    color: #ff4b4b;
    border: 1px solid #ff4b4b25;
    animation: critical-pulse 1s ease-in-out infinite;
}
.badge-deferred-price {
    background: linear-gradient(135deg, #fed33012, #ff980008);
    color: #fed330;
    border: 1px solid #fed33025;
    animation: deferred-pulse 2s ease-in-out infinite;
}
.badge-deferred-full {
    background: linear-gradient(135deg, #ff980012, #ff600008);
    color: #ff9800;
    border: 1px solid #ff980025;
    animation: deferred-pulse 2s ease-in-out infinite;
}

/* ── Diagnostics ── */
.diag-bar {
    background: #080f1a;
    border: 1px solid #0f1f30;
    border-radius: 8px;
    padding: 0.5rem 1rem;
    font-family: 'JetBrains Mono', monospace;
    font-size: 0.65rem;
    color: #2d4a62;
    display: flex;
    gap: 1.5rem;
    flex-wrap: wrap;
    margin-bottom: 1rem;
    align-items: center;
}
.diag-ok   { color: #26de81; }
.diag-err  { color: #ff4b4b; }
.diag-warn { color: #fed330; }

/* ── Streamlit metric overrides ── */
[data-testid="metric-container"] {
    background: #080f1a;
    border: 1px solid #0f1f30;
    border-radius: 8px;
    padding: 0.7rem 1rem;
}
[data-testid="stMetricValue"] {
    font-family: 'JetBrains Mono', monospace !important;
    font-size: 1.1rem !important;
    color: #e2eaf4 !important;
}
[data-testid="stMetricLabel"] {
    font-size: 0.65rem !important;
    color: #2d4a62 !important;
    text-transform: uppercase;
    letter-spacing: 0.1em;
}
</style>
""", unsafe_allow_html=True)

# ── Constants ──────────────────────────────────────────────────────────────────
BOOTSTRAP_SERVERS    = os.environ.get('KAFKA_BOOTSTRAP_SERVERS', 'localhost:9092')
TOPIC_ENERGY         = 'energy_data'
TOPIC_CARS           = 'cars_real'
TOPIC_COMMANDS       = 'charging_commands'

# Load station config — fall back gracefully if config not importable yet
try:
    _cfg_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    sys.path.insert(0, _cfg_root)
    from config.stations import STATIONS
except Exception:
    STATIONS = {
        "station_A": {"name": "Alpha",  "x": 2.0, "y": 2.0, "base_price": 50.0, "capacity": 5},
        "station_B": {"name": "Beta",   "x": 8.0, "y": 8.0, "base_price": 40.0, "capacity": 3},
        "station_C": {"name": "Gamma",  "x": 5.0, "y": 2.0, "base_price": 70.0, "capacity": 4},
    }

STATION_CAPACITY     = max(s['capacity'] for s in STATIONS.values())   # largest single station
TOTAL_CAPACITY       = sum(s['capacity'] for s in STATIONS.values())    # fleet-wide total
BASE_PRICE           = min(s['base_price'] for s in STATIONS.values())  # cheapest station base

# State-based price levels — must match produce_energy_data.py PRICE_LEVELS
# (label, occupancy_upper, multiplier, color)
PRICE_STATES = [
    ('Off-peak',  0.30, 0.6, '#26de81'),   # ≤ 30% occupied → ×0.6 → 30 EUR/MWh
    ('Normal',    0.60, 1.0, '#fed330'),   # ≤ 60% occupied → ×1.0 → 50 EUR/MWh
    ('Peak',      1.01, 1.8, '#ff4b4b'),   # > 60% occupied → ×1.8 → 90 EUR/MWh
]

PRICE_HISTORY_LEN    = 120   # ~2 min of ticks at 1 s/tick

# ── Session state ──────────────────────────────────────────────────────────────
defaults = {
    'dynamic_prices':          [],      # rolling list of current_price per tick
    'time_labels':             [],      # rolling list of formatted HH:MM strings corresponding to ticks
    'active_chargers_hist':    [],      # parallel list of active_charger counts
    'cars':                    {},      # car_id → latest telemetry dict
    'car_decisions':           {},      # car_id → 'CHARGING'|'IDLE'|'DEFERRED_PRICE'|'DEFERRED_FULL'|'EMERGENCY'
    'car_reserved_slots':      {},      # car_id → list of reserved slot indices
    'reservation_counts':      {},      # str(slot_idx) → count of reservations (aggregate)
    'projected_prices':        [],      # list of 96 floats (aggregate)
    'hourly_profile':          {},      # str(hour) → {peak_count, total_count}
    'instant_price':           None,   # last instantaneous component price
    'hist_price':              None,   # last historical component price
    'compound_boost':          False,  # whether compound boost is active
    # Multi-station fields
    'station_data':            {},      # station_id → {current_price, active_chargers, capacity, base_price, ...}
    'car_stations':            {},      # car_id → station_id (last known assignment)
    'per_station_reservation_counts': {},  # station_id → {str(slot_idx): count}
    'per_station_projected_prices':   {},  # station_id → [96 floats]
    'station_price_histories':   {},     # station_id → rolling list of prices (PRICE_HISTORY_LEN)
    'station_charger_histories': {},    # station_id → rolling list of active_chargers counts
    'station_hourly_profiles':   {},    # station_id → {str(hour): {peak_count, total_count}}
    'price_day_snapshots':     [],      # list of {'label': str, 'prices': [96 floats]}
    'last_snapshot_day':       -1,      # last day_idx we snapshotted
    # ── Q-agent stats ────────────────────────────────────────────────────────
    'q_agent_epsilon':         1.0,
    'q_agent_steps':           0,
    'q_agent_rewards':         [],
    # ── Per-group cumulative metrics (from RESERVATION_STATUS group_metrics) ─
    'gm_heuristic': {'charge_count': 0, 'emergency_count': 0,
                     'total_cost_eur': 0.0, 'soc_sum': 0.0, 'car_count': 0},
    'gm_ai':        {'charge_count': 0, 'emergency_count': 0,
                     'total_cost_eur': 0.0, 'soc_sum': 0.0, 'car_count': 0},
    # ── Per-group station distribution (from charging_commands) ──────────────
    'gm_h_station_counts':     {},      # {station_id: int}
    'gm_a_station_counts':     {},
    'kafka_ready':             False,
    'total_messages':          0,
    'poll_count':              0,
    'last_error':              None,
    'topic_counts':            {'energy_data': 0, 'cars_real': 0, 'charging_commands': 0},
}
for k, v in defaults.items():
    if k not in st.session_state:
        st.session_state[k] = v

# ── Kafka consumer (one per browser session) ───────────────────────────────────
if 'consumer' not in st.session_state:
    try:
        st.session_state['consumer'] = KafkaConsumer(
            TOPIC_ENERGY, TOPIC_CARS, TOPIC_COMMANDS,
            bootstrap_servers=BOOTSTRAP_SERVERS,
            value_deserializer=lambda x: json.loads(x.decode('utf-8')),
            auto_offset_reset='earliest',
            enable_auto_commit=False,
            group_id=f'dash_{uuid.uuid4().hex[:8]}'
        )
        st.session_state['consumer_ok'] = True
    except Exception as e:
        st.session_state['consumer'] = None
        st.session_state['consumer_ok'] = False
        st.session_state['last_error'] = str(e)

consumer    = st.session_state['consumer']
consumer_ok = st.session_state.get('consumer_ok', False)

# ── Poll Kafka ─────────────────────────────────────────────────────────────────
def poll_kafka():
    if consumer is None:
        return
    if 'topic_counts' not in st.session_state:
        st.session_state['topic_counts'] = {'energy_data': 0, 'cars_real': 0, 'charging_commands': 0}

    poll_timeout = 3000 if not st.session_state['kafka_ready'] else 100
    try:
        raw_msgs = consumer.poll(timeout_ms=poll_timeout)
        st.session_state['poll_count'] += 1
    except Exception as e:
        st.session_state['last_error'] = str(e)
        return

    msg_count = sum(len(v) for v in raw_msgs.values())
    st.session_state['total_messages'] += msg_count
    if raw_msgs:
        st.session_state['kafka_ready'] = True

    for _, messages in raw_msgs.items():
        for msg in messages:
            try:
                data  = msg.value
                topic = msg.topic

                # ── Energy / price topic ──────────────────────────────────────
                if topic == TOPIC_ENERGY:
                    if data.get('type') == 'DYNAMIC_PRICE':
                        # ── Multi-station per-station data ────────────────────
                        stations_payload = data.get('stations', {})
                        if stations_payload:
                            st.session_state['station_data'].update(stations_payload)
                            # Per-station rolling price history
                            _ph   = st.session_state['station_price_histories']
                            _ch   = st.session_state['station_charger_histories']
                            _sp_prof = st.session_state['station_hourly_profiles']
                            for _sid, _sd in stations_payload.items():
                                # price history
                                if _sid not in _ph:
                                    _ph[_sid] = []
                                _ph[_sid].append(_sd.get('current_price', 0.0))
                                if len(_ph[_sid]) > PRICE_HISTORY_LEN:
                                    _ph[_sid].pop(0)
                                # charger count history
                                if _sid not in _ch:
                                    _ch[_sid] = []
                                _ch[_sid].append(_sd.get('active_chargers', 0))
                                if len(_ch[_sid]) > PRICE_HISTORY_LEN:
                                    _ch[_sid].pop(0)
                                # learned hourly profile
                                if 'hourly_profile' in _sd:
                                    _sp_prof[_sid] = _sd['hourly_profile']

                        # Aggregate/backward-compat fields
                        price    = data.get('current_price')
                        chargers = data.get('active_chargers', 0)
                        # Derive aggregate price from station data if not explicitly provided
                        if price is None and stations_payload:
                            prices_weighted = [
                                v.get('current_price', 50.0) * v.get('capacity', 1)
                                for v in stations_payload.values()
                            ]
                            total_cap = sum(v.get('capacity', 1) for v in stations_payload.values())
                            price = sum(prices_weighted) / max(total_cap, 1)

                        if price is not None:
                            st.session_state['topic_counts']['energy_data'] += 1

                            idx = data.get('interval_idx', 0)
                            h, m = (idx * 15) // 60, (idx * 15) % 60
                            time_str = f"{h:02d}:{m:02d}"

                            st.session_state['dynamic_prices'].append(price)
                            st.session_state['time_labels'].append(time_str)
                            st.session_state['active_chargers_hist'].append(chargers)
                            # keep rolling window
                            if len(st.session_state['dynamic_prices']) > PRICE_HISTORY_LEN:
                                st.session_state['dynamic_prices'].pop(0)
                                st.session_state['time_labels'].pop(0)
                                st.session_state['active_chargers_hist'].pop(0)

                            # Capture per-component breakdown (use first station if multi)
                            if 'hourly_profile' in data:
                                st.session_state['hourly_profile'] = data['hourly_profile']
                            first_stn = next(iter(stations_payload.values()), {})
                            if 'instant_price' in first_stn:
                                st.session_state['instant_price'] = first_stn['instant_price']
                            if 'hist_price' in first_stn:
                                st.session_state['hist_price'] = first_stn['hist_price']
                            if 'compound_boost' in first_stn:
                                st.session_state['compound_boost'] = first_stn['compound_boost']
                            # Backward-compat single-station fields
                            if 'instant_price' in data:
                                st.session_state['instant_price'] = data['instant_price']
                            if 'hist_price' in data:
                                st.session_state['hist_price'] = data['hist_price']
                            if 'compound_boost' in data:
                                st.session_state['compound_boost'] = data['compound_boost']

                            # Snapshot projected prices at day boundary (idx resets to 0)
                            last_snap = st.session_state.get('last_snapshot_day', -1)
                            # Snapshot every 32 slots (~8 sim-hours) AND at midnight (idx=0)
                            SNAP_INTERVAL = 32
                            _should_snap = (
                                (idx == 0 and last_snap not in (-1, 0)) or
                                (idx > 0 and last_snap != -1 and (idx % SNAP_INTERVAL == 0) and last_snap != idx)
                            )
                            if _should_snap:
                                proj = st.session_state.get('projected_prices', [])
                                if proj and len(proj) == 96:
                                    snaps = st.session_state['price_day_snapshots']
                                    h_label = f"{(idx*15)//60:02d}:00"
                                    day_n   = len(snaps) + 1
                                    snaps.append({'label': f'Period {day_n} ({h_label})', 'prices': list(proj)})
                                    if len(snaps) > 3:
                                        snaps.pop(0)
                            st.session_state['last_snapshot_day'] = idx

                            _dash_log(f"PRICE | {price:.1f} EUR/MWh | chargers={chargers}")

                # ── Car telemetry topic ───────────────────────────────────────
                elif topic == TOPIC_CARS:
                    car_id = data['id']
                    st.session_state['cars'][car_id] = data
                    st.session_state['topic_counts']['cars_real'] += 1
                    _dash_log(f"CAR | {car_id} | soc={data.get('current_soc', 0)*100:.1f}%")

                # ── Charging commands topic ───────────────────────────────────
                elif topic == TOPIC_COMMANDS:
                    st.session_state['topic_counts']['charging_commands'] += 1

                    # Handle RESERVATION_STATUS messages
                    if data.get('type') == 'RESERVATION_STATUS':
                        st.session_state['reservation_counts'] = data.get('reservation_counts', {})
                        st.session_state['projected_prices'] = data.get('projected_prices', [])
                        # Multi-station fields
                        per_stn = data.get('reservation_counts_per_station', {})
                        if per_stn:
                            st.session_state['per_station_reservation_counts'].update(per_stn)
                        for sid, sdata in data.get('stations', {}).items():
                            pp = sdata.get('projected_prices')
                            if pp:
                                st.session_state['per_station_projected_prices'][sid] = pp
                        # Q-agent stats
                        q_stats = data.get('q_agent_stats') or {}
                        if q_stats:
                            st.session_state['q_agent_epsilon'] = q_stats.get('epsilon', 1.0)
                            st.session_state['q_agent_steps']   = q_stats.get('steps', 0)
                            hist = q_stats.get('reward_history', [])
                            if hist:
                                st.session_state['q_agent_rewards'].extend(hist)
                                if len(st.session_state['q_agent_rewards']) > 200:
                                    st.session_state['q_agent_rewards'] = \
                                        st.session_state['q_agent_rewards'][-200:]
                        # Per-group metrics (accumulate each tick)
                        for grp_key, grp_data in (data.get('group_metrics') or {}).items():
                            ss_key = f'gm_{grp_key}'
                            if ss_key in st.session_state:
                                gm = st.session_state[ss_key]
                                gm['charge_count']   += grp_data.get('charge_count', 0)
                                gm['emergency_count'] += grp_data.get('emergency_count', 0)
                                gm['total_cost_eur'] += (grp_data.get('avg_cost_eur', 0.0)
                                                         * grp_data.get('charge_count', 0))
                                gm['soc_sum']   += grp_data.get('avg_soc', 0.0) * grp_data.get('car_count', 0)
                                gm['car_count'] += grp_data.get('car_count', 0)
                        continue

                    car_id = data.get('car_id')
                    action = data.get('action', '')
                    reason = data.get('reason', '')

                    # ── Per-group station distribution ────────────────────────
                    cmd_group = data.get('group', 'heuristic')
                    sc_key    = 'gm_h_station_counts' if cmd_group == 'heuristic' else 'gm_a_station_counts'
                    if action == 'START_CHARGING':
                        sid_cmd = data.get('station_id')
                        if sid_cmd:
                            sc = st.session_state[sc_key]
                            sc[sid_cmd] = sc.get(sid_cmd, 0) + 1
                    # Track which station each car is assigned to
                    assigned_sid = data.get('station_id')
                    if car_id and assigned_sid:
                        st.session_state['car_stations'][car_id] = assigned_sid
                    if car_id:
                        if action == 'START_CHARGING':
                            label = 'EMERGENCY' if 'emergency' in reason.lower() else 'CHARGING'
                        elif action == 'STOP_CHARGING':
                            if 'full' in reason.lower():
                                label = 'DEFERRED_FULL'
                            elif 'price' in reason.lower():
                                label = 'DEFERRED_PRICE'
                            else:
                                label = 'IDLE'
                        else:
                            label = 'IDLE'
                        st.session_state['car_decisions'][car_id] = label

                        # Store per-car reserved slots
                        res_slots = data.get('reserved_slots', [])
                        if res_slots:
                            st.session_state['car_reserved_slots'][car_id] = res_slots
                        elif car_id in st.session_state['car_reserved_slots'] and not res_slots:
                            st.session_state['car_reserved_slots'].pop(car_id, None)

                        _dash_log(f"CMD | {car_id} → {label} (action={action}, reason={reason})")

            except Exception as e:
                st.session_state['last_error'] = f"msg parse: {e}"

# ── Helpers ────────────────────────────────────────────────────────────────────
def soc_color(soc):
    if soc < 0.15: return '#ff4b4b'
    if soc < 0.40: return '#fed330'
    if soc < 0.75: return '#26de81'
    return '#00e5ff'

def occupancy_color(n, total=STATION_CAPACITY):
    ratio = n / total if total else 0
    if ratio == 0:   return '#00e5ff'
    if ratio <= 0.4: return '#26de81'
    if ratio <= 0.8: return '#fed330'
    return '#ff4b4b'

# ── Poll + compute ─────────────────────────────────────────────────────────────
poll_kafka()

dynamic_prices       = st.session_state['dynamic_prices']
active_chargers_hist = st.session_state['active_chargers_hist']
current_price        = dynamic_prices[-1]  if dynamic_prices       else None
current_chargers     = active_chargers_hist[-1] if active_chargers_hist else 0
charging_count       = sum(1 for c in st.session_state['cars'].values() if c.get('plugged_in'))

# ══════════════════════════════════════════════════════════════════════════════
#  LAYOUT
# ══════════════════════════════════════════════════════════════════════════════
st.markdown('<div class="dash-title">⚡ EV Fleet Control Center</div>', unsafe_allow_html=True)

# ── Diagnostics bar ────────────────────────────────────────────────────────────
kafka_cls = "diag-ok"  if consumer_ok                     else "diag-err"
ready_cls = "diag-ok"  if st.session_state['kafka_ready'] else "diag-warn"
err_html  = (f'<span class="diag-err">ERR: {st.session_state["last_error"]}</span>'
             if st.session_state['last_error'] else '')
st.markdown(f"""
<div class="diag-bar">
  <span><span class="{kafka_cls}">{'● KAFKA OK' if consumer_ok else '● KAFKA FAIL'}</span></span>
  <span><span class="{ready_cls}">{'● READY' if st.session_state['kafka_ready'] else '● WARMING UP…'}</span></span>
  <span>POLLS: {st.session_state['poll_count']}</span>
  <span>ENERGY: {st.session_state['topic_counts']['energy_data']}</span>
  <span>CARS: {st.session_state['topic_counts']['cars_real']}</span>
  <span>CMDS: {st.session_state['topic_counts']['charging_commands']}</span>
  <span>HOST: {BOOTSTRAP_SERVERS}</span>
  {err_html}
</div>
""", unsafe_allow_html=True)


station_data = st.session_state.get('station_data', {})

# ── Agent Comparison Tabs ─────────────────────────────────────────────────────
_epsilon = st.session_state.get('q_agent_epsilon', 1.0)
_steps   = st.session_state.get('q_agent_steps', 0)
_rewards = st.session_state.get('q_agent_rewards', [])

_gm_h = st.session_state.get('gm_heuristic', {})
_gm_a = st.session_state.get('gm_ai', {})
_h_sc = st.session_state.get('gm_h_station_counts', {})
_a_sc = st.session_state.get('gm_a_station_counts', {})

def _gm_avg_cost(gm):
    return gm['total_cost_eur'] / gm['charge_count'] if gm.get('charge_count') else 0.0

def _gm_avg_soc(gm):
    return gm['soc_sum'] / gm['car_count'] * 100 if gm.get('car_count') else 0.0

def _sc_str(sc):
    return "  ".join(
        f"{sid.replace('station_','').upper()}:{cnt}"
        for sid, cnt in sorted(sc.items())
    ) or "—"

def _group_active_per_station(group_prefix):
    """Count currently-charging cars per station for a given group (h_car_ or a_car_)."""
    cars_ss     = st.session_state.get('cars', {})
    car_stn_ss  = st.session_state.get('car_stations', {})
    counts = {sid: 0 for sid in STATIONS}
    for cid, cdata in cars_ss.items():
        if cid.startswith(group_prefix) and cdata.get('plugged_in'):
            sid = car_stn_ss.get(cid)
            if sid and sid in counts:
                counts[sid] += 1
    return counts

def _render_station_row(active_per_station):
    """Render station cards showing per-group active charger counts."""
    _cols = st.columns(len(STATIONS))
    for _col, (sid, stn_cfg) in zip(_cols, STATIONS.items()):
        with _col:
            _sd        = station_data.get(sid, {})
            _sp        = _sd.get('current_price', stn_cfg['base_price'])
            _sa        = active_per_station.get(sid, 0)
            _cap       = stn_cfg['capacity']
            _occ_col   = occupancy_color(_sa, _cap)
            _price_col = ('#ff4b4b' if _sp > stn_cfg['base_price'] * 1.4
                          else '#26de81' if _sp <= stn_cfg['base_price'] * 1.05
                          else '#fed330')
            st.markdown(f"""
            <div style="background:#080f1a;border:1px solid #0f1f30;border-radius:10px;
                        padding:0.6rem 0.8rem;margin-bottom:0.5rem;border-left:3px solid {_occ_col}">
              <div style="font-family:JetBrains Mono,monospace;font-size:0.52rem;color:#4a6070;
                          letter-spacing:0.12em;text-transform:uppercase">{stn_cfg['name']}</div>
              <div style="display:flex;align-items:baseline;gap:6px;margin-top:2px">
                <span style="font-family:JetBrains Mono,monospace;font-size:1.05rem;
                             font-weight:700;color:{_price_col}">{_sp:.1f}</span>
                <span style="font-size:0.58rem;color:#4a6070">€/MWh</span>
              </div>
              <div style="display:flex;align-items:center;gap:6px;margin-top:4px">
                <div style="flex:1;height:5px;background:#0a1525;border-radius:2px;overflow:hidden">
                  <div style="height:100%;width:{int(_sa/_cap*100) if _cap else 0}%;
                              background:{_occ_col};border-radius:2px;transition:width 0.5s"></div>
                </div>
                <span style="font-family:JetBrains Mono,monospace;font-size:0.65rem;
                             font-weight:600;color:{_occ_col}">{_sa}/{_cap} charging</span>
              </div>
            </div>
            """, unsafe_allow_html=True)

def _render_group_metrics(gm, sc):
    avg_cost = _gm_avg_cost(gm)
    avg_soc  = _gm_avg_soc(gm)
    emerg    = gm.get('emergency_count', 0)
    charges  = gm.get('charge_count', 0)
    c1, c2, c3, c4 = st.columns(4)
    c1.metric("Avg cost/slot", f"{avg_cost:.4f} €")
    c2.metric("Emergencies",   str(emerg))
    c3.metric("Avg SOC",       f"{avg_soc:.1f}%")
    c4.metric("Charge events", str(charges))
    if sc:
        st.caption(f"Cumulative station distribution: {_sc_str(sc)}")

def _render_main_dashboard(key_sfx, group_prefix):
    """Render the two-column panels, filtered to one car group."""
    # ── Shared charger schedule (used by Reservation Board + Charging Stations) ───
    _car_res = st.session_state.get('car_reserved_slots', {})
    _car_res_active = {k: v for k, v in _car_res.items() if v}
    
    charger_schedule = [{} for _ in range(STATION_CAPACITY)]   # charger_idx → {slot: car_id}
    car_to_charger = {}                                         # car_id → charger_idx
    
    if _car_res_active:
        _sorted_cars = sorted(_car_res_active.items(), key=lambda x: (min(x[1]), x[0]))
        for _cid, _slots in _sorted_cars:
            _slots_set = set(_slots)
            _best = None
            for _ch in range(STATION_CAPACITY):
                if not (_slots_set & set(charger_schedule[_ch].keys())):
                    _best = _ch
                    break
            if _best is not None:
                for _s in _slots:
                    charger_schedule[_best][_s] = _cid
                car_to_charger[_cid] = _best
            else:
                for _s in _slots:
                    for _ch in range(STATION_CAPACITY):
                        if _s not in charger_schedule[_ch]:
                            charger_schedule[_ch][_s] = _cid
                            car_to_charger.setdefault(_cid, _ch)
                            break
    

    # ── Main columns ───────────────────────────────────────────────────────────────
    left_col, right_col = st.columns([3, 2], gap="medium")
    
    # ═══════════════════════════════════════════════
    #  LEFT — Price Sparkline + Price Ladder
    # ═══════════════════════════════════════════════
    with left_col:
    
        # ── Dynamic price sparkline ──────────────────────────────────────────────
        st.markdown('<div class="section-hdr">Real-Time Price Feed (€/MWh)</div>', unsafe_allow_html=True)
    
        _stn_histories = st.session_state.get('station_price_histories', {})
        _t_labels      = list(st.session_state['time_labels'])
        # Per-station line colours (one per station, stable order)
        _STN_LINE_COLS = ['#00e5ff', '#26de81', '#fed330']
        _has_any_history = any(len(v) > 0 for v in _stn_histories.values())
    
        if _has_any_history:
            fig = go.Figure()
            # Coloured background bands for price zones (based on cheapest station)
            fig.add_hrect(y0=0, y1=BASE_PRICE*PRICE_STATES[0][2],
                          fillcolor='rgba(38,222,129,0.04)', line_width=0)
            fig.add_hrect(y0=BASE_PRICE*PRICE_STATES[0][2], y1=BASE_PRICE*PRICE_STATES[1][2],
                          fillcolor='rgba(254,211,48,0.04)', line_width=0)
            fig.add_hrect(y0=BASE_PRICE*PRICE_STATES[1][2], y1=BASE_PRICE*PRICE_STATES[2][2]*1.5,
                          fillcolor='rgba(255,75,75,0.04)', line_width=0)
    
            _all_prices = []
            for _i, (_sid, _stn_cfg) in enumerate(STATIONS.items()):
                _prices = _stn_histories.get(_sid, [])
                if not _prices:
                    continue
                _all_prices.extend(_prices)
                _n   = len(_prices)
                _xs  = list(range(-_n + 1, 1))
                _col = _STN_LINE_COLS[_i % len(_STN_LINE_COLS)]
                _tlbs = _t_labels[-_n:] + [''] * max(0, _n - len(_t_labels))
                fig.add_trace(go.Scatter(
                    x=_xs, y=_prices,
                    mode='lines',
                    name=f"{_stn_cfg['name']} ({_stn_cfg['base_price']:.0f}€ base)",
                    line=dict(color=_col, width=2),
                    customdata=_tlbs,
                    hovertemplate=f"<b>{_stn_cfg['name']}</b><br>%{{customdata}}<br>%{{y:.1f}} €/MWh<extra></extra>",
                ))
    
            # Threshold reference lines (dotted, one per price state)
            for _lbl, _, _mult, _col in PRICE_STATES:
                fig.add_hline(
                    y=BASE_PRICE * _mult, line_dash='dot', line_color=_col, line_width=0.8,
                    annotation_text=f'{_lbl} {BASE_PRICE*_mult:.0f}€',
                    annotation_font_color=_col, annotation_font_size=8,
                    annotation_position='right',
                )
    
            _y_max = max(max(_all_prices) * 1.15,
                         max(s['base_price'] for s in STATIONS.values()) * PRICE_STATES[2][2] * 1.1)
            fig.update_layout(
                height=230,
                paper_bgcolor='#0d1520', plot_bgcolor='#0d1520',
                margin=dict(l=50, r=110, t=10, b=35),
                xaxis=dict(
                    tickfont=dict(color='#4a6070', size=9),
                    gridcolor='#1e2d3d', linecolor='#1e2d3d', zeroline=False, title=None,
                ),
                yaxis=dict(
                    title='€/MWh', tickfont=dict(color='#4a6070', size=9),
                    title_font=dict(color='#4a6070', size=9),
                    gridcolor='#1e2d3d', linecolor='#1e2d3d', zeroline=False,
                    range=[0, _y_max],
                ),
                legend=dict(
                    orientation='h', yanchor='bottom', y=1.01, xanchor='left', x=0,
                    font=dict(color='#4a6070', size=9), bgcolor='rgba(0,0,0,0)',
                ),
            )
            st.plotly_chart(fig, use_container_width=True, config={'displayModeBar': False},
                            key=f'price_sparkline_{key_sfx}')
        else:
            st.info("⏳ Waiting for dynamic price data…")
    
        # ── Active charger count history — per station ────────────────────────────
        _stn_chg_hist = st.session_state.get('station_charger_histories', {})
        _t_lbls2      = list(st.session_state['time_labels'])
        if any(len(v) > 1 for v in _stn_chg_hist.values()):
            fig2 = go.Figure()
            for _i, (_sid, _stn_cfg) in enumerate(STATIONS.items()):
                _chist = _stn_chg_hist.get(_sid, [])
                if len(_chist) < 2:
                    continue
                _col  = _STN_LINE_COLS[_i % len(_STN_LINE_COLS)]
                _n    = len(_chist)
                _xs   = list(range(-_n + 1, 1))
                _tlbs = _t_lbls2[-_n:] + [''] * max(0, _n - len(_t_lbls2))
                fig2.add_trace(go.Scatter(
                    x=_xs, y=_chist,
                    mode='lines',
                    name=f"{_stn_cfg['name']} (cap {_stn_cfg['capacity']})",
                    line=dict(color=_col, width=1.5),
                    customdata=_tlbs,
                    hovertemplate=(f"<b>{_stn_cfg['name']}</b><br>"
                                   f"%{{customdata}}<br>%{{y}} / {_stn_cfg['capacity']} chargers"
                                   f"<extra></extra>"),
                ))
            # Capacity reference lines per station
            for _i, (_sid, _stn_cfg) in enumerate(STATIONS.items()):
                _col = _STN_LINE_COLS[_i % len(_STN_LINE_COLS)]
                fig2.add_hline(y=_stn_cfg['capacity'], line_dash='dot',
                               line_color=_col, line_width=0.6, opacity=0.4)
            fig2.update_layout(
                height=120,
                paper_bgcolor='#0d1520', plot_bgcolor='#0d1520',
                margin=dict(l=50, r=10, t=5, b=30),
                xaxis=dict(tickfont=dict(color='#4a6070', size=9),
                           gridcolor='#1e2d3d', linecolor='#1e2d3d',
                           zeroline=False, title=None),
                yaxis=dict(
                    title='Chargers busy',
                    tickfont=dict(color='#4a6070', size=9),
                    title_font=dict(color='#4a6070', size=9),
                    gridcolor='#1e2d3d', linecolor='#1e2d3d',
                    zeroline=False,
                    range=[0, max(s['capacity'] for s in STATIONS.values()) + 0.5],
                    dtick=1,
                ),
                legend=dict(orientation='h', yanchor='bottom', y=1.01, xanchor='left', x=0,
                            font=dict(color='#4a6070', size=8), bgcolor='rgba(0,0,0,0)'),
            )
            st.plotly_chart(fig2, use_container_width=True, config={'displayModeBar': False},
                            key=f'charger_history_{key_sfx}')
    
        # ── Learned Hourly Profile — per station ─────────────────────────────────
        st.markdown('<div class="section-hdr">What the Model Learned — Peak Frequency by Hour</div>',
                    unsafe_allow_html=True)
    
        _stn_profiles = st.session_state.get('station_hourly_profiles', {})
        _prof_tabs    = st.tabs([f"{cfg['name']} ({sid.replace('station_','')})"
                                 for sid, cfg in STATIONS.items()])
    
        _cur_hour_idx = (st.session_state.get('last_snapshot_day', 48) * 15) // 60 % 24
        _off_m = PRICE_STATES[0][2]
        _peak_m = PRICE_STATES[2][2]
        _hours   = list(range(24))
        _h_labels = [f'{h:02d}:00' for h in _hours]
    
        for _tab, (_sid, _stn_cfg) in zip(_prof_tabs, STATIONS.items()):
            with _tab:
                _profile = _stn_profiles.get(_sid, {})
                _base    = _stn_cfg['base_price']
                _peak_freqs, _bar_colors, _obs_counts, _hist_prices = [], [], [], []
                for h in _hours:
                    _p   = _profile.get(str(h), {})
                    _tot = _p.get('total_count', 0)
                    _pk  = _p.get('peak_count', 0)
                    _obs_counts.append(_tot)
                    if _tot >= 4:
                        _freq = _pk / _tot
                        _hp   = _base * (_off_m + (_peak_m - _off_m) * _freq)
                    else:
                        _freq = None
                        _hp   = None
                    _peak_freqs.append(_freq)
                    _hist_prices.append(_hp)
                    if _freq is None:
                        _bar_colors.append('#1a2d44')
                    elif _freq >= 0.6:
                        _bar_colors.append('#ff4b4b')
                    elif _freq >= 0.25:
                        _bar_colors.append('#fed330')
                    else:
                        _bar_colors.append('#26de81')
    
                _fig_p = go.Figure()
                _fig_p.add_trace(go.Bar(
                    x=_h_labels,
                    y=[f if f is not None else 0 for f in _peak_freqs],
                    marker_color=_bar_colors,
                    customdata=[(_obs_counts[i], _hist_prices[i] or 0) for i in _hours],
                    hovertemplate=(
                        '<b>%{x}</b><br>'
                        'Peak freq: %{y:.0%}<br>'
                        'Learned price: %{customdata[1]:.1f} €/MWh<br>'
                        'Observations: %{customdata[0]}<extra></extra>'
                    ),
                ))
                _fig_p.add_vline(x=_cur_hour_idx,
                                 line_dash='solid', line_color='rgba(255,255,255,0.3)', line_width=1.5,
                                 annotation_text='Now', annotation_font_color='#e2eaf4',
                                 annotation_font_size=9)
                _fig_p.add_hline(y=0.6,  line_dash='dot', line_color='#ff4b4b', line_width=0.8,
                                 annotation_text='→ Peak',   annotation_font_color='#ff4b4b',
                                 annotation_font_size=8, annotation_position='right')
                _fig_p.add_hline(y=0.25, line_dash='dot', line_color='#fed330', line_width=0.8,
                                 annotation_text='→ Normal', annotation_font_color='#fed330',
                                 annotation_font_size=8, annotation_position='right')
                _fig_p.update_layout(
                    height=190,
                    paper_bgcolor='#0d1520', plot_bgcolor='#0d1520',
                    margin=dict(l=50, r=90, t=8, b=38),
                    xaxis=dict(tickfont=dict(color='#4a6070', size=8), tickangle=-45,
                               gridcolor='#1e2d3d', linecolor='#1e2d3d', zeroline=False, title=None),
                    yaxis=dict(title='% peak', tickformat='.0%',
                               tickfont=dict(color='#4a6070', size=9),
                               title_font=dict(color='#4a6070', size=9),
                               gridcolor='#1e2d3d', linecolor='#1e2d3d', zeroline=False,
                               range=[0, 1.05]),
                    showlegend=False, bargap=0.15,
                )
                st.plotly_chart(_fig_p, use_container_width=True, config={'displayModeBar': False},
                                key=f'peak_freq_{_sid}_{key_sfx}')
                st.markdown(
                    f'<div style="font-family:JetBrains Mono,monospace;font-size:0.57rem;color:#2d4a62;'
                    f'margin-top:-0.4rem">Base {_base:.0f} €/MWh · '
                    f'Peak threshold {_base*_peak_m:.0f} €/MWh · '
                    f'Drives the 40 % predictive pricing component for {_stn_cfg["name"]}.</div>',
                    unsafe_allow_html=True,
                )
    
        # ── Bidding decision legend ───────────────────────────────────────────────
        st.markdown("""
        <div style="display:flex;gap:1.2rem;font-size:0.64rem;color:#2d4a62;margin-top:1rem;font-family:JetBrains Mono,monospace;align-items:center;flex-wrap:wrap">
          <span><span style="display:inline-block;width:8px;height:8px;border-radius:2px;background:#00e5ff;margin-right:5px;vertical-align:middle"></span>CHARGING</span>
          <span><span style="display:inline-block;width:8px;height:8px;border-radius:2px;background:#ff4b4b;margin-right:5px;vertical-align:middle"></span>EMERGENCY</span>
          <span><span style="display:inline-block;width:8px;height:8px;border-radius:2px;background:#fed330;margin-right:5px;vertical-align:middle"></span>DEFERRED (price too high)</span>
          <span><span style="display:inline-block;width:8px;height:8px;border-radius:2px;background:#ff9800;margin-right:5px;vertical-align:middle"></span>DEFERRED (station full)</span>
          <span><span style="display:inline-block;width:8px;height:8px;border-radius:2px;background:#2d4a62;margin-right:5px;vertical-align:middle"></span>IDLE</span>
        </div>
        """, unsafe_allow_html=True)
    
        # ── SOC-band willingness-to-pay reference ─────────────────────────────────
        st.markdown('<div class="section-hdr">Willingness-to-Pay by SOC Band</div>', unsafe_allow_html=True)
    
        soc_bands = [
            ('<15%',   '< 15%',    'Emergency — charges at any price',              '#ff4b4b', 1.0),
            ('15-30%', '15 – 30%', f'Critical — pays up to {BASE_PRICE*2.5:.0f} €', '#fed330', 2.5),
            ('30-50%', '30 – 50%', f'Low — pays up to {BASE_PRICE*1.8:.0f} €',      '#26de81', 1.8),
            ('>50%',   '> 50%',    'Waiting — does not charge',                      '#2d4a62', 0.0),
        ]
    
        soc_html = '<div style="display:flex;flex-direction:column;gap:4px">'
        for band_key, soc_label, desc, col, mult in soc_bands:
            fill_pct = int(mult / PRICE_STATES[-1][2] * 100)
            soc_html += f'''
            <div style="background:#080f1a;border:1px solid #0f1f30;border-radius:8px;padding:0.4rem 0.8rem;display:flex;align-items:center;gap:12px">
              <div style="font-family:JetBrains Mono,monospace;font-size:0.65rem;color:{col};width:55px;flex-shrink:0">{soc_label}</div>
              <div style="flex:1;height:5px;background:#0a1525;border-radius:3px;overflow:hidden">
                <div style="width:{fill_pct}%;height:100%;background:{col};border-radius:3px;opacity:0.75"></div>
              </div>
              <div style="font-family:JetBrains Mono,monospace;font-size:0.62rem;color:#4a6070;width:260px;flex-shrink:0;text-align:right">{desc}</div>
            </div>'''
        soc_html += '</div>'
        st.markdown(soc_html, unsafe_allow_html=True)
    
        # ── Reservation Board — grouped per station ───────────────────────────────
        st.markdown('<div class="section-hdr">Reservation Board</div>', unsafe_allow_html=True)
    
        _car_stations_res = st.session_state.get('car_stations', {})
        _charger_colors   = ['#00e5ff', '#26de81', '#fed330', '#ff9800', '#7c5cbf']
        _any_reservations = False
    
        for _sid, _stn_cfg in STATIONS.items():
            _stn_cap  = _stn_cfg['capacity']
            _stn_name = _stn_cfg['name']
    
            # Cars with reservations assigned to this station
            _stn_car_res = {
                cid: slots for cid, slots in _car_res_active.items()
                if _car_stations_res.get(cid) == _sid
            }
            if not _stn_car_res:
                continue
            _any_reservations = True
    
            # Build a charger schedule for just this station
            _stn_schedule = [{} for _ in range(_stn_cap)]
            _sorted_stn = sorted(_stn_car_res.items(), key=lambda x: (min(x[1]), x[0]))
            for _cid, _slots in _sorted_stn:
                _slots_set = set(_slots)
                _best = None
                for _ch in range(_stn_cap):
                    if not (_slots_set & set(_stn_schedule[_ch].keys())):
                        _best = _ch
                        break
                if _best is not None:
                    for _s in _slots:
                        _stn_schedule[_best][_s] = _cid
                else:
                    for _s in _slots:
                        for _ch in range(_stn_cap):
                            if _s not in _stn_schedule[_ch]:
                                _stn_schedule[_ch][_s] = _cid
                                break
    
            _all_slots = sorted({s for ch in _stn_schedule for s in ch})
            _sd        = station_data.get(_sid, {})
            _sp        = _sd.get('current_price', _stn_cfg['base_price'])
            _pc        = ('#ff4b4b' if _sp > _stn_cfg['base_price'] * 1.4
                          else '#26de81' if _sp <= _stn_cfg['base_price'] * 1.05
                          else '#fed330')
    
            tbl  = (f'<div style="font-family:JetBrains Mono,monospace;font-size:0.55rem;'
                    f'color:{_pc};letter-spacing:0.1em;margin:0.6rem 0 0.25rem">'
                    f'⚡ {_sid.upper().replace("_"," ")} — {_stn_name.upper()} '
                    f'({_sp:.1f} €/MWh · {_stn_cap} chargers)</div>')
            tbl += '<div style="overflow-x:auto;margin-bottom:0.5rem">'
            tbl += ('<table style="border-collapse:collapse;width:100%;font-family:JetBrains Mono,'
                    'monospace;font-size:0.58rem">')
    
            # Header — time slots
            tbl += ('<tr><td style="padding:4px 8px;color:#2d4a62;border-bottom:1px solid #0f1f30;'
                    'position:sticky;left:0;background:#060b14;z-index:1;min-width:80px">CHARGER</td>')
            for _s in _all_slots:
                _sh, _sm = (_s * 15) // 60, (_s * 15) % 60
                tbl += (f'<td style="padding:4px 5px;color:#2d4a62;border-bottom:1px solid #0f1f30;'
                        f'text-align:center;white-space:nowrap">{_sh:02d}:{_sm:02d}</td>')
            tbl += '</tr>'
    
            # One row per charger
            for _ch in range(_stn_cap):
                _ch_col = _charger_colors[_ch % len(_charger_colors)]
                tbl += (f'<tr><td style="padding:4px 8px;color:{_ch_col};border-bottom:1px solid #0f1f30;'
                        f'position:sticky;left:0;background:#060b14;z-index:1;white-space:nowrap">'
                        f'&#9889; CHARGER {_ch + 1}</td>')
                for _s in _all_slots:
                    _cid = _stn_schedule[_ch].get(_s)
                    if _cid is not None:
                        _short = _cid.upper().replace('CAR_', 'C')
                        _cd    = st.session_state['cars'].get(_cid, {})
                        _soc   = float(_cd.get('current_soc', 0.5))
                        _sc    = soc_color(_soc)
                        tbl += (f'<td style="padding:3px 4px;text-align:center;border-bottom:1px solid #0f1f30;'
                                f'background:rgba(0,0,0,0.2);color:{_sc};white-space:nowrap" '
                                f'title="{_cid} — SOC {_soc*100:.0f}%">{_short}</td>')
                    else:
                        tbl += (f'<td style="padding:3px 4px;text-align:center;border-bottom:1px solid #0f1f30;'
                                f'color:#0f1f30">-</td>')
                tbl += '</tr>'
    
            # Load row
            tbl += ('<tr><td style="padding:4px 8px;color:#4a6070;border-top:1px solid #1a2d44;'
                    'position:sticky;left:0;background:#060b14;z-index:1;font-weight:500">LOAD</td>')
            for _s in _all_slots:
                _cnt = sum(1 for _ch in range(_stn_cap) if _s in _stn_schedule[_ch])
                _pct = int(_cnt / _stn_cap * 100)
                _bc  = '#26de81' if _cnt / _stn_cap <= 0.4 else '#fed330' if _cnt / _stn_cap <= 0.8 else '#ff4b4b'
                tbl += (f'<td style="padding:3px 4px;text-align:center;border-top:1px solid #1a2d44">'
                        f'<div style="color:{_bc};font-weight:500;margin-bottom:2px">{_cnt}/{_stn_cap}</div>'
                        f'<div style="height:3px;background:#0a1525;border-radius:2px;overflow:hidden">'
                        f'<div style="height:100%;width:{_pct}%;background:{_bc};border-radius:2px"></div>'
                        f'</div></td>')
            tbl += '</tr></table></div>'
            st.markdown(tbl, unsafe_allow_html=True)
    
        if not _any_reservations:
            st.markdown(
                '<div style="font-family:JetBrains Mono,monospace;font-size:0.65rem;color:#2d4a62;'
                'padding:0.8rem;background:#080f1a;border:1px solid #0f1f30;border-radius:8px;'
                'text-align:center">No active reservations</div>',
                unsafe_allow_html=True)
    
    
        # ── 2D Fleet Grid Map ─────────────────────────────────────────────────────
        st.markdown('<div class="section-hdr">Fleet Grid Map</div>', unsafe_allow_html=True)
    
        _car_stations = st.session_state.get('car_stations', {})
        _all_cars = st.session_state.get('cars', {})
    
        # Car scatter data
        _cx, _cy, _csoc, _cid_list, _ccol, _ctext = [], [], [], [], [], []
        for _cid, _cd in _all_cars.items():
            _cx.append(_cd.get('x', 5.0))
            _cy.append(_cd.get('y', 5.0))
            _soc_v = _cd.get('current_soc', 0.5)
            _csoc.append(_soc_v * 100)
            _cid_list.append(_cid)
            _ccol.append(soc_color(_soc_v))
            _ctext.append(f"{_cid} — SOC {_soc_v*100:.0f}%")
    
        fig_map = go.Figure()
    
        # Merge all travel lines into ONE trace (None separators = disconnected segments).
        # A fixed trace count avoids Streamlit re-rendering the whole chart when charger
        # count changes between ticks, which was causing the double-map ghost.
        _line_x, _line_y = [], []
        for _cid, _sid in _car_stations.items():
            _cd  = _all_cars.get(_cid, {})
            _stn = STATIONS.get(_sid, {})
            if _cd and _stn and _cd.get('plugged_in'):
                _line_x += [_cd.get('x', 5.0), _stn['x'], None]
                _line_y += [_cd.get('y', 5.0), _stn['y'], None]
        fig_map.add_trace(go.Scatter(
            x=_line_x, y=_line_y,
            mode='lines',
            line=dict(color='rgba(0,229,255,0.13)', width=1),
            hoverinfo='skip',
            showlegend=False,
        ))
    
        # Car dots
        fig_map.add_trace(go.Scatter(
            x=_cx, y=_cy,
            mode='markers',
            marker=dict(
                size=7,
                color=_csoc,
                colorscale=[[0,'#ff4b4b'],[0.15,'#ff4b4b'],[0.4,'#fed330'],[0.75,'#26de81'],[1.0,'#00e5ff']],
                cmin=0, cmax=100,
                line=dict(width=0.5, color='#1a2d44'),
            ),
            text=_ctext,
            hovertemplate='%{text}<extra></extra>',
            name='Cars',
            showlegend=False,
        ))
    
        # Station markers
        _STN_COLORS = ['#00e5ff', '#26de81', '#fed330']
        for _i, (sid, stn) in enumerate(STATIONS.items()):
            _sd   = station_data.get(sid, {})
            _sa   = _sd.get('active_chargers', 0)
            _cap  = stn['capacity']
            _sc   = occupancy_color(_sa, _cap)
            _sp   = _sd.get('current_price', stn['base_price'])
            fig_map.add_trace(go.Scatter(
                x=[stn['x']], y=[stn['y']],
                mode='markers+text',
                marker=dict(size=18, color=_sc, symbol='square', line=dict(width=2, color='#060b14')),
                text=[stn['name']],
                textposition='top center',
                textfont=dict(color='#e2eaf4', size=9, family='JetBrains Mono'),
                hovertemplate=f"{sid}<br>{stn['name']}<br>Price: {_sp:.1f} €/MWh<br>Chargers: {_sa}/{_cap}<extra></extra>",
                name=stn['name'],
                showlegend=False,
            ))
    
        fig_map.update_layout(
            height=280,
            paper_bgcolor='#0d1520', plot_bgcolor='#0d1520',
            margin=dict(l=20, r=20, t=10, b=20),
            xaxis=dict(range=[-0.5, 10.5], gridcolor='#1e2d3d', zeroline=False,
                       tickfont=dict(color='#4a6070', size=8), title='km'),
            yaxis=dict(range=[-0.5, 10.5], gridcolor='#1e2d3d', zeroline=False,
                       tickfont=dict(color='#4a6070', size=8), title='km',
                       scaleanchor='x', scaleratio=1),
        )
        st.plotly_chart(fig_map, use_container_width=True, config={'displayModeBar': False},
                        key=f'fleet_grid_map_{key_sfx}')
    
    
    
    # ═══════════════════════════════════════════════
    #  RIGHT — Charging Stations + Fleet Status
    # ═══════════════════════════════════════════════
    with right_col:
    
        # ── Charging station slots ────────────────────────────────────────────────
        st.markdown('<div class="section-hdr">Charging Stations</div>', unsafe_allow_html=True)
    
        cars = st.session_state['cars']
        # Build charger → car mapping using the shared schedule
        # For each charger, find the plugged-in car assigned to it (if any)
        _charger_to_car = {}
        _plugged_in = {cid for cid, c in cars.items() if c.get('plugged_in', False)}
        for cid in _plugged_in:
            ch = car_to_charger.get(cid)
            if ch is not None:
                _charger_to_car[ch] = cid
            else:
                # Car is plugged in but has no reservation — assign to first free charger
                for _ch in range(STATION_CAPACITY):
                    if _ch not in _charger_to_car:
                        _charger_to_car[_ch] = cid
                        break
    
        # Render one group of charger cards per station
        _car_stations_map = st.session_state.get('car_stations', {})
        _station_html = ''
        for _sid, _stn_cfg in STATIONS.items():
            _stn_cap  = _stn_cfg['capacity']
            _stn_name = _stn_cfg['name']
            _sd       = station_data.get(_sid, {})
            _sp       = _sd.get('current_price', _stn_cfg['base_price'])
            _price_col = ('#ff4b4b' if _sp > _stn_cfg['base_price'] * 1.4
                          else '#26de81' if _sp <= _stn_cfg['base_price'] * 1.05
                          else '#fed330')
            _station_html += (
                f'<div style="font-family:JetBrains Mono,monospace;font-size:0.55rem;'
                f'color:{_price_col};letter-spacing:0.1em;margin:0.5rem 0 0.3rem">'
                f'⚡ {_sid.upper().replace("_"," ")} — {_stn_name.upper()} '
                f'({_sp:.1f} €/MWh)</div>'
            )
            _station_html += '<div style="display:flex;gap:8px;flex-wrap:wrap;margin-bottom:0.6rem">'
            # Cars at this station that are currently plugged in
            _at_this_stn = [cid for cid, s in _car_stations_map.items()
                            if s == _sid and cars.get(cid, {}).get('plugged_in', False)
                            and cid.startswith(group_prefix)]
            for _slot in range(_stn_cap):
                if _slot < len(_at_this_stn):
                    _cid  = _at_this_stn[_slot]
                    _soc  = float(cars[_cid].get('current_soc', 0.0)) * 100
                    _label = _cid.upper().replace('_', ' ')
                    _station_html += f'''
                    <div style="flex:1;min-width:70px;background:#080f1a;border:1px solid {_price_col}40;
                                border-radius:10px;padding:0.6rem 0.5rem;text-align:center;
                                box-shadow:0 0 10px {_price_col}18;animation:pulse-border 2.5s ease-in-out infinite">
                      <div style="font-size:1.3rem;margin-bottom:2px">⚡</div>
                      <div style="font-family:JetBrains Mono,monospace;font-size:0.6rem;color:{_price_col};
                                  letter-spacing:0.08em;margin-bottom:4px">{_label}</div>
                      <div style="font-family:JetBrains Mono,monospace;font-size:1rem;color:#e2eaf4;
                                  font-weight:500">{_soc:.0f}<span style="font-size:0.65rem;color:#4a6070">%</span></div>
                      <div style="height:4px;background:#0a1525;border-radius:2px;margin-top:5px;overflow:hidden">
                        <div style="height:100%;width:{_soc:.0f}%;background:{_price_col};border-radius:2px;
                                     background-size:200% auto;animation:batt-shimmer 2s linear infinite"></div>
                      </div>
                    </div>'''
                else:
                    _station_html += f'''
                    <div style="flex:1;min-width:70px;background:#080f1a;border:1px solid #0f1f30;
                                border-radius:10px;padding:0.6rem 0.5rem;text-align:center;opacity:0.5">
                      <div style="font-size:1.3rem;margin-bottom:2px;filter:grayscale(1)">🔌</div>
                      <div style="font-family:JetBrains Mono,monospace;font-size:0.6rem;color:#2d4a62;
                                  letter-spacing:0.08em;margin-bottom:4px">SLOT {_slot + 1}</div>
                      <div style="font-family:JetBrains Mono,monospace;font-size:0.8rem;color:#2d4a62">FREE</div>
                      <div style="height:4px;background:#0a1525;border-radius:2px;margin-top:5px"></div>
                    </div>'''
            _station_html += '</div>'
        st.markdown(_station_html, unsafe_allow_html=True)
    
        # ── Routing Decision Matrix ───────────────────────────────────────────────
        st.markdown('<div class="section-hdr">Routing Decision Matrix</div>', unsafe_allow_html=True)
        st.markdown(
            '<div style="font-family:JetBrains Mono,monospace;font-size:0.55rem;color:#2d4a62;'
            'margin-bottom:0.5rem">For each assigned car: travel cost + charging cost per station. '
            'The chosen station has the lowest total — showing why it beat cheaper-but-farther '
            'or closer-but-pricier alternatives.</div>',
            unsafe_allow_html=True)
    
        _rdm_cars  = {k: v for k, v in st.session_state.get('car_stations', {}).items()
                       if k.startswith(group_prefix)}
        _rdm_res   = st.session_state.get('car_reserved_slots', {})
        _rdm_res_counts = st.session_state.get('per_station_reservation_counts', {})

        def _projected_price_for_station(sid: str, res_count: int) -> float:
            """Mirror of Flink's projected_price_for_station — occupancy-based price."""
            base  = STATIONS[sid]['base_price']
            cap   = STATIONS[sid]['capacity']
            ratio = res_count / max(cap, 1)
            for _, upper, mult, _ in PRICE_STATES:
                if ratio <= upper:
                    return base * mult
            return base * PRICE_STATES[-1][2]

        def _cheapest_slots_charge_eur(sid: str, n_slots: int, current_idx: int) -> float:
            """Compute charging cost using cheapest projected future slots — matches Flink."""
            counts   = _rdm_res_counts.get(sid, {})
            cap      = STATIONS[sid]['capacity']
            candidates = []
            for slot in range(current_idx, 96):
                cnt = int(counts.get(str(slot), 0))
                if cnt >= cap:
                    continue
                price = _projected_price_for_station(sid, cnt)
                candidates.append(price)
            candidates.sort()
            cheapest = candidates[:n_slots]
            if not cheapest:
                # fallback: use base price if no slot data available
                cheapest = [STATIONS[sid]['base_price']] * n_slots
            return sum(p * 2.75 / 1000 for p in cheapest)
    
        # Show all cars with active reserved slots, sorted by SOC asc
        _rdm_active = [
            (cid, sid) for cid, sid in _rdm_cars.items()
            if cid in cars and _rdm_res.get(cid)
        ]
        _rdm_active.sort(key=lambda x: cars.get(x[0], {}).get('current_soc', 1.0))
    
        if _rdm_active:
            _stn_ids   = list(STATIONS.keys())
            _stn_short = {'station_A': 'Alpha', 'station_B': 'Beta', 'station_C': 'Gamma'}
    
            rdm_html = '<div style="overflow-x:auto">'
            # Table header
            rdm_html += ('<table style="border-collapse:collapse;width:100%;font-family:JetBrains Mono,'
                         'monospace;font-size:0.57rem;white-space:nowrap">')
            rdm_html += '<tr style="color:#2d4a62;border-bottom:1px solid #0f1f30">'
            rdm_html += '<td style="padding:3px 6px">CAR</td><td style="padding:3px 6px">SOC</td>'
            for _sid in _stn_ids:
                _scfg = STATIONS[_sid]
                rdm_html += (f'<td colspan="3" style="padding:3px 8px;text-align:center;'
                             f'border-left:1px solid #0f1f30">'
                             f'{_scfg["name"].upper()} '
                             f'<span style="color:#1a3048">({_scfg["base_price"]:.0f}€ base)</span>'
                             f'</td>')
            rdm_html += '</tr>'
            # Sub-header
            rdm_html += '<tr style="color:#1a3048;border-bottom:1px solid #0f1f30;font-size:0.5rem">'
            rdm_html += '<td colspan="2"></td>'
            for _sid in _stn_ids:
                rdm_html += ('<td style="padding:2px 5px;border-left:1px solid #0f1f30;text-align:right">Travel€</td>'
                             '<td style="padding:2px 5px;text-align:right">Charge€</td>'
                             '<td style="padding:2px 5px;text-align:right;font-weight:600">Total€</td>')
            rdm_html += '</tr>'
    
            for _cid, _chosen_sid in _rdm_active[:20]:   # cap at 20 rows
                _car   = cars.get(_cid, {})
                _soc   = float(_car.get('current_soc', 0.5))
                _cx    = float(_car.get('x', 5.0))
                _cy    = float(_car.get('y', 5.0))
                _slots = _rdm_res.get(_cid, [])
                _n_slots = max(len(_slots), 1)
                _soc_c = soc_color(_soc)
                _label = _cid.upper().replace('CAR_', 'C')
                _is_emg = st.session_state['car_decisions'].get(_cid) == 'EMERGENCY'
    
                # Pre-compute costs for each station using projected occupancy prices
                _cur_idx = st.session_state.get('interval_idx', 0) or 0
                _costs = {}
                for _sid in _stn_ids:
                    _scfg  = STATIONS[_sid]
                    _dist  = math.sqrt((_cx - _scfg['x'])**2 + (_cy - _scfg['y'])**2)
                    _trav  = 2 * _dist * 0.2 * _scfg['base_price'] / 1000
                    _chg   = _cheapest_slots_charge_eur(_sid, _n_slots, _cur_idx)
                    _costs[_sid] = (_trav, _chg, _trav + _chg, _dist)
    
                # Find cheapest total (what the algorithm would choose for normal cars)
                _best_sid = min(_costs, key=lambda s: _costs[s][2])
    
                rdm_html += f'<tr style="border-bottom:1px solid #0a1520">'
                rdm_html += (f'<td style="padding:3px 6px;color:{_soc_c}">'
                             f'{"⚠ " if _is_emg else ""}{_label}</td>')
                rdm_html += (f'<td style="padding:3px 6px;color:{_soc_c};text-align:right">'
                             f'{_soc*100:.0f}%</td>')
    
                for _sid in _stn_ids:
                    _trav, _chg, _total, _dist = _costs[_sid]
                    _is_best   = (_sid == _best_sid)
                    _cell_bg   = 'background:#0d1f10;' if _is_best else ''
                    _col       = '#26de81' if _is_best else '#4a6070'
                    _tick      = ' ✓' if _is_best else ''
                    rdm_html += (
                        f'<td style="padding:3px 5px;border-left:1px solid #0f1f30;'
                        f'text-align:right;color:#4a6070;{_cell_bg}">'
                        f'<span title="{_dist:.1f} km">{_trav:.3f}</span></td>'
                        f'<td style="padding:3px 5px;text-align:right;color:#4a6070;{_cell_bg}">'
                        f'{_chg:.3f}</td>'
                        f'<td style="padding:3px 5px;text-align:right;font-weight:600;'
                        f'color:{_col};{_cell_bg}">{_total:.3f}{_tick}</td>'
                    )
                rdm_html += '</tr>'
    
            rdm_html += '</table></div>'
            st.markdown(rdm_html, unsafe_allow_html=True)
            st.markdown(
                '<div style="font-family:JetBrains Mono,monospace;font-size:0.52rem;color:#1a3048;'
                'margin-top:0.3rem">Travel€ = 2 × distance × 0.2kWh/km × base_price · '
                'Charge€ = cheapest projected slots × 2.75kWh × occupancy_price · ✓ = chosen station</div>',
                unsafe_allow_html=True)
        else:
            st.markdown(
                '<div style="font-family:JetBrains Mono,monospace;font-size:0.65rem;color:#2d4a62;'
                'padding:0.6rem;background:#080f1a;border:1px solid #0f1f30;border-radius:8px;'
                'text-align:center">No active routing decisions</div>',
                unsafe_allow_html=True)
    
        # ── Fleet status (car cards) ──────────────────────────────────────────────
        st.markdown('<div class="section-hdr">Fleet Status</div>', unsafe_allow_html=True)
    
        DECISION_BADGE = {
            'CHARGING':       ('&#9889; CHG',    'badge-charging'),
            'EMERGENCY':      ('&#9888; EMERGENCY', 'badge-critical'),
            'DEFERRED_PRICE': ('&#8987; PRICE',  'badge-deferred-price'),
            'DEFERRED_FULL':  ('&#8987; FULL',   'badge-deferred-full'),
            'IDLE':           ('&#9679; IDLE',   'badge-idle'),
        }
    
        if cars:
            all_cards_html = ""
            for car_id, car in sorted(cars.items()):
                if not car_id.startswith(group_prefix):
                    continue
                try:
                    soc         = float(car.get('current_soc', 0.0))
                    soc_pct     = round(soc * 100, 1)
                    plugged     = bool(car.get('plugged_in', False))
                    priority    = str(car.get('priority', '-'))
                    kwh         = float(car.get('current_battery_kwh', 0.0))
                    fill_color  = soc_color(soc)
                    is_critical = soc < 0.15
                    batt_cls    = 'charging' if plugged else ''
    
                    # Decision badge from latest Flink command
                    decision       = st.session_state['car_decisions'].get(car_id, 'IDLE')
                    badge_html, badge_cls = DECISION_BADGE.get(decision, ('&#9679; IDLE', 'badge-idle'))
    
                    # Station badge
                    _assigned_sid  = st.session_state.get('car_stations', {}).get(car_id)
                    _stn_name_short = STATIONS.get(_assigned_sid, {}).get('name', '')[:1] if _assigned_sid else ''
                    _stn_badge = (f'<span style="font-family:JetBrains Mono,monospace;font-size:0.5rem;'
                                  f'color:#7c5cbf;background:#7c5cbf18;border:1px solid #7c5cbf30;'
                                  f'border-radius:3px;padding:1px 4px;margin-right:4px">→{_stn_name_short}</span>'
                                  if _stn_name_short else '')
    
                    label      = car_id.upper().replace("_", " ")
                    border_col = ('#00e5ff30' if plugged else
                                  '#ff4b4b30' if is_critical else
                                  '#fed33030' if 'DEFERRED' in decision else '#0f1f30')
                    row_anim   = ('animation:pulse-border 2.5s ease-in-out infinite;' if plugged else
                                  'animation:critical-pulse 1.5s ease-in-out infinite;' if is_critical else '')
    
                    all_cards_html += (
                        f'<div style="display:flex;align-items:center;gap:8px;padding:0.35rem 0.7rem;'
                        f'margin-bottom:4px;background:#080f1a;border:1px solid {border_col};'
                        f'border-radius:8px;{row_anim}">'
                          # Car ID
                          f'<div style="font-family:JetBrains Mono,monospace;font-size:0.62rem;'
                          f'color:#4a6070;width:70px;flex-shrink:0;letter-spacing:0.06em">{label}</div>'
                          # SOC %
                          f'<div style="font-family:JetBrains Mono,monospace;font-size:0.9rem;'
                          f'font-weight:500;color:{fill_color};width:44px;flex-shrink:0;text-align:right">'
                          f'{soc_pct:.0f}<span style="font-size:0.6rem;color:#2d4a62">%</span></div>'
                          # Battery bar
                          f'<div style="flex:1;height:5px;background:#0a1525;border-radius:3px;overflow:hidden;min-width:40px">'
                            f'<div class="batt-fill {batt_cls}" style="height:100%;width:{soc_pct}%;'
                            f'background:{fill_color};border-radius:3px"></div>'
                          f'</div>'
                          # kWh
                          f'<div style="font-family:JetBrains Mono,monospace;font-size:0.6rem;'
                          f'color:#2d4a62;width:46px;flex-shrink:0;text-align:right">{kwh:.1f}kWh</div>'
                          # Priority
                          f'<div style="font-family:JetBrains Mono,monospace;font-size:0.6rem;'
                          f'color:#4a6070;width:28px;flex-shrink:0;text-align:center">P{priority}</div>'
                          # Station badge + Decision badge
                          f'<div style="flex-shrink:0">{_stn_badge}<span class="badge {badge_cls}" '
                          f'style="font-size:0.55rem;padding:0.15rem 0.45rem">{badge_html}</span></div>'
                        f'</div>'
                    )
                except Exception as e:
                    all_cards_html += f'<div style="color:#ff4b4b;font-size:0.8rem;padding:0.5rem">Error [{car_id}]: {e}</div>'
    
            st.markdown(all_cards_html, unsafe_allow_html=True)
    
        else:
            st.info("Waiting for car telemetry…")
            st.caption(f"Consumer ready: {st.session_state['kafka_ready']} | Messages: {st.session_state['total_messages']}")
    


_view = st.radio(
    "view", ["Heuristic", "AI Agent", "Comparison"],
    horizontal=True, label_visibility="collapsed",
    key="main_view",
)

# ── Heuristic view ────────────────────────────────────────────────────────────
if _view == "Heuristic":
    st.markdown('<div class="section-hdr">Station Activity — Heuristic Group</div>',
                unsafe_allow_html=True)
    _render_station_row(_group_active_per_station('h_car_'))
    st.markdown('<div class="section-hdr">Performance Metrics</div>', unsafe_allow_html=True)
    st.caption("Strategy: minimises travel_EUR + charging_EUR across all slot combinations using NetworkX graph")
    _render_group_metrics(_gm_h, _h_sc)
    if st.button("Reset heuristic metrics", key="reset_h"):
        st.session_state['gm_heuristic']        = {'charge_count': 0, 'emergency_count': 0,
                                                     'total_cost_eur': 0.0, 'soc_sum': 0.0, 'car_count': 0}
        st.session_state['gm_h_station_counts'] = {}
    _render_main_dashboard('h', 'h_car_')

# ── AI Agent view ─────────────────────────────────────────────────────────────
elif _view == "AI Agent":
    st.markdown('<div class="section-hdr">Station Activity — AI Agent Group</div>',
                unsafe_allow_html=True)
    _render_station_row(_group_active_per_station('a_car_'))
    st.markdown('<div class="section-hdr">Q-Learning Status</div>', unsafe_allow_html=True)
    _ai_col1, _ai_col2 = st.columns([1, 2])
    with _ai_col1:
        st.metric("Exploration ε", f"{_epsilon:.3f}")
        st.metric("Q-updates",     f"{_steps:,}")
        st.caption("ε decays 0.80 → 0.10 as the agent learns")
    with _ai_col2:
        if _rewards:
            st.caption("Reward history (last 200 Q-updates)")
            st.line_chart(_rewards, height=110, use_container_width=True)
        else:
            st.caption("Waiting for AI agent data…")
    st.markdown('<div class="section-hdr">Performance Metrics</div>', unsafe_allow_html=True)
    st.caption("Strategy: ε-greedy Q-learning — learns which station gives best reward (cheap + near + available)")
    _render_group_metrics(_gm_a, _a_sc)
    if st.button("Reset AI metrics", key="reset_a"):
        st.session_state['gm_ai']               = {'charge_count': 0, 'emergency_count': 0,
                                                     'total_cost_eur': 0.0, 'soc_sum': 0.0, 'car_count': 0}
        st.session_state['gm_a_station_counts'] = {}
        st.session_state['q_agent_rewards']     = []
    _render_main_dashboard('ai', 'a_car_')

# ── Comparison view ───────────────────────────────────────────────────────────
else:
    st.markdown('<div class="section-hdr">Heuristic vs AI Agent — Live Comparison</div>',
                unsafe_allow_html=True)
    st.caption("Identical cars, same starting conditions, same prices — only the decision algorithm differs")
    _h_avg_cost = _gm_avg_cost(_gm_h)
    _a_avg_cost = _gm_avg_cost(_gm_a)
    _h_avg_soc  = _gm_avg_soc(_gm_h)
    _a_avg_soc  = _gm_avg_soc(_gm_a)
    _h_emerg    = _gm_h.get('emergency_count', 0)
    _a_emerg    = _gm_a.get('emergency_count', 0)

    def _delta_str(h_val, a_val, fmt='.4f', unit=''):
        d = a_val - h_val
        sign = '−' if d < 0 else '+'
        return f"{sign}{abs(d):{fmt}}{unit}"

    _cmp_df = pd.DataFrame({
        'Metric':     ['Avg cost/slot (€)', 'Emergencies', 'Avg SOC (%)',
                       'Charge events', 'Station distribution'],
        'Heuristic':  [f"{_h_avg_cost:.4f}", str(_h_emerg),
                       f"{_h_avg_soc:.1f}%", str(_gm_h.get('charge_count', 0)), _sc_str(_h_sc)],
        'AI Agent':   [f"{_a_avg_cost:.4f}", str(_a_emerg),
                       f"{_a_avg_soc:.1f}%", str(_gm_a.get('charge_count', 0)), _sc_str(_a_sc)],
        'AI − Heuristic': [
            _delta_str(_h_avg_cost, _a_avg_cost, '.4f'),
            _delta_str(_h_emerg,    _a_emerg,    '.0f'),
            _delta_str(_h_avg_soc,  _a_avg_soc,  '.1f', 'pp'),
            '—', '—',
        ],
    })
    st.dataframe(_cmp_df, hide_index=True, use_container_width=True)

    if _gm_h.get('charge_count') or _gm_a.get('charge_count'):
        import plotly.express as _px
        _bar_df = pd.DataFrame({
            'Group': ['Heuristic', 'AI Agent'],
            'Avg cost/slot (€)': [_h_avg_cost, _a_avg_cost],
        })
        _fig_bar = _px.bar(
            _bar_df, x='Group', y='Avg cost/slot (€)', color='Group',
            color_discrete_map={'Heuristic': '#00e5ff', 'AI Agent': '#26de81'},
            template='plotly_dark',
        )
        _fig_bar.update_layout(
            paper_bgcolor='#060b14', plot_bgcolor='#060b14',
            showlegend=False, height=260, margin=dict(t=20, b=10, l=10, r=10),
        )
        st.plotly_chart(_fig_bar, use_container_width=True, key='cmp_bar_cost')

# ── Auto-refresh ───────────────────────────────────────────────────────────────
time.sleep(1)
st.rerun()