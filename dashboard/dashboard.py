import os

import streamlit as st
import time
import json
import pandas as pd
import numpy as np
import plotly.graph_objects as go
from kafka import KafkaConsumer
import uuid
import sys
# dashboard/ -> go up one level to project root
_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__))) if '__file__' in dir() else '.'
sys.path.insert(0, _root)
try:
    from ev_logger import log
    _dash_log = lambda msg: log('DASH', msg)
except Exception:
    _dash_log = lambda msg: None

# ── Page Config ────────────────────────────────────────────────────────────────
st.set_page_config(page_title="EV Fleet Control", layout="wide", page_icon="⚡")

# ── Global CSS ─────────────────────────────────────────────────────────────────
st.markdown("""
<style>
@import url('https://fonts.googleapis.com/css2?family=Inter:wght@300;400;500;600&family=JetBrains+Mono:wght@400;500&display=swap');

/* ── Base ── */
html, body, [class*="css"] {
    font-family: 'Inter', sans-serif;
    background-color: #060b14;
    color: #8ba3bc;
}
.stApp { background-color: #060b14; }

/* ── Scrollbar ── */
::-webkit-scrollbar { width: 4px; }
::-webkit-scrollbar-track { background: #0a1220; }
::-webkit-scrollbar-thumb { background: #1a2d44; border-radius: 2px; }

/* ── Hide Streamlit chrome ── */
#MainMenu, footer, header { visibility: hidden; }
.block-container { padding-top: 1.5rem; padding-bottom: 1rem; }

/* ── Keyframes ── */
@keyframes pulse-border {
    0%   { box-shadow: 0 0 0 1px #00e5ff18, 0 4px 24px #00e5ff08; border-color: #00e5ff30; }
    50%  { box-shadow: 0 0 0 1px #00e5ff55, 0 4px 32px #00e5ff20; border-color: #00e5ff80; }
    100% { box-shadow: 0 0 0 1px #00e5ff18, 0 4px 24px #00e5ff08; border-color: #00e5ff30; }
}
@keyframes scan-line {
    0%   { transform: translateY(-100%); opacity: 0; }
    15%  { opacity: 0.8; }
    85%  { opacity: 0.8; }
    100% { transform: translateY(500%); opacity: 0; }
}
@keyframes bolt-flash {
    0%, 100% { opacity: 1; }
    50%       { opacity: 0.5; }
}
@keyframes batt-shimmer {
    0%   { background-position: -200% center; }
    100% { background-position: 200% center; }
}
@keyframes critical-pulse {
    0%, 100% { box-shadow: 0 0 0 1px #ff4b4b20, 0 4px 16px #ff4b4b08; border-color: #ff4b4b30; }
    50%       { box-shadow: 0 0 0 1px #ff4b4b60, 0 4px 24px #ff4b4b25; border-color: #ff4b4b70; }
}
@keyframes fade-in {
    from { opacity: 0; transform: translateY(4px); }
    to   { opacity: 1; transform: translateY(0); }
}

/* ── Title ── */
.dash-title {
    font-family: 'JetBrains Mono', monospace;
    font-size: 1.1rem;
    font-weight: 500;
    letter-spacing: 0.2em;
    color: #00e5ff;
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
    padding: 0;
    margin-bottom: 1.2rem;
    overflow: hidden;
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
    letter-spacing: 0.05em;
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

/* ── Car card ── */
.car-card {
    background: #080f1a;
    border: 1px solid #0f1f30;
    border-radius: 12px;
    padding: 1rem 1.1rem;
    margin-bottom: 0.8rem;
    position: relative;
    overflow: hidden;
    transition: border-color 0.4s ease, box-shadow 0.4s ease;
    animation: fade-in 0.3s ease;
}
.car-card::before {
    content: '';
    position: absolute;
    top: 0; left: 0; right: 0;
    height: 1px;
    background: linear-gradient(90deg, transparent, #1a3048, transparent);
}
.car-card.charging {
    border-color: #00e5ff30;
    animation: pulse-border 2.5s ease-in-out infinite;
}
.car-card.charging::after {
    content: '';
    position: absolute;
    top: 0; left: 0; right: 0;
    height: 1px;
    background: linear-gradient(90deg, transparent, #00e5ff, transparent);
    animation: scan-line 3s ease-in-out infinite;
}
.car-card.critical {
    border-color: #ff4b4b30;
    animation: critical-pulse 1.5s ease-in-out infinite;
}

.car-id {
    font-family: 'JetBrains Mono', monospace;
    font-size: 0.7rem;
    font-weight: 500;
    color: #2d4a62;
    letter-spacing: 0.12em;
    text-transform: uppercase;
    margin-bottom: 0.3rem;
}
.car-soc-num {
    font-family: 'JetBrains Mono', monospace;
    font-size: 2.2rem;
    font-weight: 500;
    line-height: 1;
    letter-spacing: -0.02em;
}
.car-meta {
    font-size: 0.7rem;
    font-weight: 400;
    color: #1e3348;
    margin-top: 0.25rem;
    letter-spacing: 0.02em;
}

/* ── Battery bar ── */
.batt-track {
    height: 6px;
    background: #0a1525;
    border-radius: 3px;
    margin: 0.7rem 0 0.4rem;
    overflow: hidden;
    position: relative;
}
.batt-fill {
    height: 100%;
    border-radius: 3px;
    transition: width 0.8s cubic-bezier(0.4, 0, 0.2, 1);
    position: relative;
}
.batt-fill.charging {
    background-size: 200% auto;
    animation: batt-shimmer 2s linear infinite;
}
.batt-fill.charging::after {
    content: '';
    position: absolute;
    inset: 0;
    background: repeating-linear-gradient(
        90deg,
        transparent,
        transparent 8px,
        rgba(255,255,255,0.12) 8px,
        rgba(255,255,255,0.12) 16px
    );
    border-radius: 3px;
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
}

/* ── Schedule row ── */
.sched-row {
    display: flex;
    gap: 2px;
    margin-top: 0.5rem;
    flex-wrap: wrap;
}
.sched-slot {
    height: 5px;
    width: 7px;
    border-radius: 2px;
    flex-shrink: 0;
    transition: background 0.3s ease;
}

/* ── Price legend ── */
.legend-row {
    display: flex;
    gap: 1.2rem;
    font-size: 0.64rem;
    color: #2d4a62;
    margin-bottom: 0.6rem;
    font-family: 'JetBrains Mono', monospace;
    align-items: center;
}
.legend-dot {
    display: inline-block;
    width: 8px; height: 8px;
    border-radius: 2px;
    margin-right: 5px;
    vertical-align: middle;
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
BOOTSTRAP_SERVERS      = os.environ.get('KAFKA_BOOTSTRAP_SERVERS', 'localhost:9092')
TOPIC_ENERGY           = 'energy_data'
TOPIC_CARS             = 'cars_real'
TOPIC_COMMANDS         = 'charging_commands'
SIM_MINUTES_PER_REAL_SEC = 12   # 1 real sec = 12 sim min → full day = 120 real sec

# ── Session State ──────────────────────────────────────────────────────────────
defaults = {
    'sim_start_wall': time.time(),   # wall-clock anchor — never changes
    'sim_start_mins': 720,           # sim minutes at anchor point — start at 12:00
    'day_count': 1,
    'energy_prices': [],             # rolling last-100 raw prices
    'today_prices_96': [],           # exactly 96 slots for today's heatmap (TODAY label only)
    'tomorrow_prices_96': [],        # exactly 96 slots for tomorrow's heatmap (TOMORROW label)
    'cars': {},
    'charging_plans': {},            # car_id → list of planned interval indices (0-191)
    'flink_interval_idx': None,      # latest interval_idx reported by Flink — used to sync cur_idx
    'kafka_ready': False,
    'total_messages': 0,
    'poll_count': 0,
    'last_error': None,
    'topic_counts': {'energy_data': 0, 'cars_real': 0, 'charging_commands': 0},
    'last_energy_batch_wall': None,  # wall-clock time when last TOMORROW batch completed (96 msgs)
    'next_batch_interval': 159,      # real seconds between TOMORROW batches (actual sim-day on ARM64 Docker)
    'today_charging_cost': 0.0,      # EUR spent on charging today — smart cars (resets at midnight)
    'total_charging_cost': 0.0,      # EUR spent on charging all-time — smart cars
    'today_test_cost': 0.0,          # EUR spent on charging today — naive test cars
    'total_test_cost': 0.0,          # EUR spent on charging all-time — naive test cars
    'today_driven_real': 0.0,        # sim-hours driven today — smart cars
    'total_driven_real': 0.0,        # sim-hours driven all-time — smart cars
    'today_driven_test': 0.0,        # sim-hours driven today — naive test cars
    'total_driven_test': 0.0,        # sim-hours driven all-time — naive test cars
}
for k, v in defaults.items():
    if k not in st.session_state:
        st.session_state[k] = v

# ── Kafka — one consumer per browser session, reads from earliest to catch up ──
# unique group_id per session so no stale committed offsets from previous runs
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
    # Guard: ensure topic_counts exists in case session pre-dates this key
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

                if topic == TOPIC_ENERGY:
                    price    = data.get('AT_price_day_ahead')
                    msg_type = data.get('type', 'TODAY')
                    _dash_log(f"ENERGY msg | type={msg_type} | price={price} | today_buf={len(st.session_state['today_prices_96'])} | tmrw_buf={len(st.session_state['tomorrow_prices_96'])}")
                    if price is not None:
                        st.session_state['topic_counts']['energy_data'] += 1
                        st.session_state['energy_prices'].append(price)
                        if len(st.session_state['energy_prices']) > 100:
                            st.session_state['energy_prices'].pop(0)
                        if msg_type == 'TODAY':
                            st.session_state['today_prices_96'].append(price)
                            if len(st.session_state['today_prices_96']) > 96:
                                st.session_state['today_prices_96'] = st.session_state['today_prices_96'][-96:]
                        else:
                            st.session_state['tomorrow_prices_96'].append(price)
                            if len(st.session_state['tomorrow_prices_96']) > 96:
                                st.session_state['tomorrow_prices_96'] = st.session_state['tomorrow_prices_96'][-96:]
                    if msg_type == 'TOMORROW' and len(st.session_state['tomorrow_prices_96']) == 1:
                        st.session_state['sim_start_wall'] = time.time()
                        st.session_state['sim_start_mins'] = 13 * 60
                    # Record wall-clock time when each TOMORROW batch completes (all 96 msgs)
                    # so the countdown timer can show seconds until the next batch.
                    if msg_type == 'TOMORROW' and len(st.session_state['tomorrow_prices_96']) % 96 == 0 and len(st.session_state['tomorrow_prices_96']) > 0:
                        st.session_state['last_energy_batch_wall'] = time.time()

                elif topic == TOPIC_CARS:
                    car_id = data['id']
                    st.session_state['cars'][car_id] = data
                    st.session_state['topic_counts']['cars_real'] += 1
                    _dash_log(f"CAR | {car_id} | soc={data.get('current_soc',0)*100:.1f}% | plugged={data.get('plugged_in')}")
                    # Accumulate driven time: each tick = 15 sim-min = 0.25 sim-hours
                    if not data.get('plugged_in', False):
                        if car_id.endswith('_test'):
                            st.session_state['today_driven_test'] += 0.25
                            st.session_state['total_driven_test'] += 0.25
                        else:
                            st.session_state['today_driven_real'] += 0.25
                            st.session_state['total_driven_real'] += 0.25

                elif topic == TOPIC_COMMANDS:
                    st.session_state['topic_counts']['charging_commands'] += 1
                    car_id = data.get('car_id')
                    plan   = data.get('plan', [])
                    interval_idx = data.get('interval_idx')
                    if interval_idx is not None:
                        prev_idx = st.session_state.get('flink_interval_idx')
                        # Detect midnight: idx resets to 0 from any value > 0.
                        # Checking only prev==95 is fragile — the poll cycle can miss that
                        # exact message, leaving prev at 94 or 93 when 0 arrives.
                        if prev_idx is not None and prev_idx > 0 and interval_idx == 0:
                            if st.session_state['tomorrow_prices_96']:
                                st.session_state['today_prices_96']    = st.session_state['tomorrow_prices_96'][:]
                                st.session_state['tomorrow_prices_96'] = []
                                _dash_log('MIDNIGHT SYNC — today/tomorrow prices rotated from Flink signal')
                            # Clear stale yesterday intervals from plans so today starts fresh
                            for cid in list(st.session_state['charging_plans'].keys()):
                                st.session_state['charging_plans'][cid] = [
                                    i - 96 for i in st.session_state['charging_plans'][cid] if i >= 96
                                ]
                            st.session_state['day_count'] = st.session_state['day_count'] + 1
                            st.session_state['today_charging_cost'] = 0.0
                            st.session_state['today_test_cost']     = 0.0
                            st.session_state['today_driven_real']   = 0.0
                            st.session_state['today_driven_test']   = 0.0
                        st.session_state['flink_interval_idx'] = interval_idx
                    # Accumulate charging cost from START_CHARGING events
                    action         = data.get('action')
                    charging_price = data.get('charging_price')
                    if action == 'START_CHARGING' and charging_price is not None:
                        # 11 kW × 0.25 h = 2.75 kWh per interval; price in EUR/MWh → EUR
                        cost = charging_price * 2.75 / 1000.0
                        if car_id and car_id.endswith('_test'):
                            st.session_state['today_test_cost']  += cost
                            st.session_state['total_test_cost']  += cost
                        else:
                            st.session_state['today_charging_cost'] += cost
                            st.session_state['total_charging_cost'] += cost
                    if car_id:
                        new_intervals = [s['interval'] for s in plan]
                        flink_idx_now = data.get('interval_idx', 0)
                        existing      = st.session_state['charging_plans'].get(car_id, [])
                        # Preserve past today slots as display history (dark teal).
                        # Replace future today and all tomorrow slots with Flink's latest plan.
                        past_today = [i for i in existing if i < 96 and i < flink_idx_now]
                        today_new  = [i for i in new_intervals if i < 96]
                        tmrw_new   = [i for i in new_intervals if i >= 96]
                        intervals  = sorted(set(past_today + today_new + tmrw_new))
                        st.session_state['charging_plans'][car_id] = intervals
                        _dash_log(f"PLAN received | {car_id} | {len(intervals)} slots | first8={intervals[:8]}")

            except Exception as e:
                st.session_state['last_error'] = f"msg parse: {e}"

# ── Helpers ────────────────────────────────────────────────────────────────────
def soc_color(soc):
    if soc < 0.15: return '#ff4b4b'
    if soc < 0.40: return '#fed330'
    if soc < 0.75: return '#26de81'
    return '#00e5ff'

def price_color(price, p10, p30, p70):
    if price <= p10:  return '#00e5ff'   # very cheap  — cyan
    if price <= p30:  return '#26de81'   # cheap       — green
    if price <= p70:  return '#fed330'   # normal      — yellow
    return '#ff4b4b'                     # expensive   — red

def slot_color_for_car(t, planned_slots, prices, p10, p30, p70):
    """Color a 15-min slot: cyan/green if planned, price-tinted if cheap, dark otherwise."""
    if t in planned_slots:
        return '#00e5ff'
    if t < len(prices):
        p = prices[t]
        if p <= p30:
            return '#26de8140'
        return '#1e2d3d'
    return '#131e2b'

# ── Advance sim clock (wall-clock anchored — display only) ───────────────────
# IMPORTANT: day_count and today/tomorrow price rotation are handled EXCLUSIVELY
# by Flink's interval_idx==0 signal in poll_kafka(). The wall-clock anchor
# (sim_start_wall) can be stale across deploy runs, so we never trigger price
# shifts or day increments here — only update the display sim_time_minutes.
def advance_clock():
    elapsed_real_secs = time.time() - st.session_state['sim_start_wall']
    total_sim_mins    = st.session_state['sim_start_mins'] + elapsed_real_secs * SIM_MINUTES_PER_REAL_SEC
    st.session_state['sim_time_minutes'] = int(total_sim_mins % 1440)

# ═══════════════════════════════════════════════════════════════════════════════
#  POLL FIRST — so first frame already has data
# ═══════════════════════════════════════════════════════════════════════════════
advance_clock()
poll_kafka()

# ── Pre-compute price stats ────────────────────────────────────────────────────
prices       = st.session_state['energy_prices']
today_p96    = st.session_state['today_prices_96']
tomorrow_p96 = st.session_state['tomorrow_prices_96']
# Use today's 96-slot buffer for percentiles if full, else fall back to rolling feed
effective_today_ref = today_p96 if len(today_p96) >= 10 else prices
ref_prices   = effective_today_ref
has_prices   = len(ref_prices) > 0
p10 = p30 = p70 = 0
if has_prices:
    p10 = float(np.percentile(ref_prices, 10))
    p30 = float(np.percentile(ref_prices, 30))
    p70 = float(np.percentile(ref_prices, 70))

# ── Sim time ───────────────────────────────────────────────────────────────────
# Prefer Flink's own interval counter — avoids wall-clock drift vs Flink's processing timer
flink_idx = st.session_state.get('flink_interval_idx')
if flink_idx is not None:
    cur_idx  = flink_idx
    _sim_mins = flink_idx * 15
else:
    cur_idx   = st.session_state['sim_time_minutes'] // 15
    _sim_mins = st.session_state['sim_time_minutes']
hours    = _sim_mins // 60
mins     = _sim_mins % 60
time_str = f"{hours:02d}:{mins:02d}"

# ══════════════════════════════════════════════════════════════════════════════
#  LAYOUT
# ══════════════════════════════════════════════════════════════════════════════

st.markdown('<div class="dash-title">⚡ EV Fleet Control Center</div>', unsafe_allow_html=True)

# ── Diagnostics bar ────────────────────────────────────────────────────────────
kafka_cls  = "diag-ok"  if consumer_ok                         else "diag-err"
ready_cls  = "diag-ok"  if st.session_state['kafka_ready']     else "diag-warn"
err_html   = f'<span class="diag-err">ERR: {st.session_state["last_error"]}</span>' if st.session_state['last_error'] else ''
st.markdown(f"""
<div class="diag-bar">
  <span><span class="{kafka_cls}">{'● KAFKA OK' if consumer_ok else '● KAFKA FAIL'}</span></span>
  <span><span class="{ready_cls}">{'● CONSUMER READY' if st.session_state['kafka_ready'] else '● WARMING UP…'}</span></span>
  <span>POLLS: {st.session_state['poll_count']}</span>
  <span>ENERGY: {st.session_state['topic_counts']['energy_data']}</span>
  <span>CARS: {st.session_state['topic_counts']['cars_real']}</span>
  <span>CMDS: {st.session_state['topic_counts']['charging_commands']}</span>
  <span>HOST: {BOOTSTRAP_SERVERS}</span>
  {err_html}
</div>
""", unsafe_allow_html=True)

# ── Clock strip ────────────────────────────────────────────────────────────────
charging_count = sum(1 for c in st.session_state['cars'].values() if c.get('plugged_in'))
last_price     = (today_p96[cur_idx] if today_p96 and cur_idx < len(today_p96)
                  else (prices[-1] if prices else (ref_prices[-1] if ref_prices else None)))
price_col      = '#ff4b4b' if has_prices and last_price > p70 else '#26de81' if has_prices and last_price <= p30 else '#fed330'

# ── Next-batch countdown ──
_last_batch = st.session_state.get('last_energy_batch_wall')
if _last_batch is not None:
    _elapsed   = time.time() - _last_batch
    _remaining = max(0.0, st.session_state['next_batch_interval'] - _elapsed)
    _mins      = int(_remaining) // 60
    _secs      = int(_remaining) % 60
    _cdval     = f"{_mins}:{_secs:02d}"
    _cdcol     = '#26de81' if _remaining > 30 else '#fed330' if _remaining > 10 else '#ff4b4b'
    _cd_cell   = f'<div class="clock-cell"><div class="clock-label">Next Energy Batch</div><div class="clock-val" style="color:{_cdcol}">{_cdval}</div></div>'
else:
    _cd_cell   = '<div class="clock-cell"><div class="clock-label">Next Energy Batch</div><div class="clock-val" style="color:#2d4a62">—</div></div>'

st.markdown(f"""
<div class="clock-strip">
  <div class="clock-cell">
    <div class="clock-label">Sim Time</div>
    <div class="clock-val accent">{time_str}</div>
  </div>
  <div class="clock-cell">
    <div class="clock-label">Day</div>
    <div class="clock-val">{st.session_state['day_count']}</div>
  </div>
  <div class="clock-cell">
    <div class="clock-label">Interval</div>
    <div class="clock-val">{cur_idx:02d} <span style="font-size:1rem;color:#1a3048">/ 96</span></div>
  </div>
  <div class="clock-cell">
    <div class="clock-label">Charging Now</div>
    <div class="clock-val">{charging_count}</div>
  </div>
  <div class="clock-cell">
    <div class="clock-label">Current Price</div>
    <div class="clock-val" style="color:{price_col}">{'—' if not has_prices else f'{last_price:.0f} <span style="font-size:1rem">€</span>'}</div>
  </div>
  {_cd_cell}
</div>
""", unsafe_allow_html=True)

# ── Main columns ───────────────────────────────────────────────────────────────
left_col, right_col = st.columns([3, 2], gap="medium")

# ═══════════════════════════════════════════════
#  LEFT — Price Chart + Schedule Heatmap
# ═══════════════════════════════════════════════
with left_col:

    # ── Price chart ──
    st.markdown('<div class="section-hdr">Energy Price Feed (€/MWh)</div>', unsafe_allow_html=True)

    if has_prices:
        # legend
        st.markdown("""
        <div class="legend-row">
          <span><span class="legend-dot" style="background:#00e5ff"></span>Very cheap (p10)</span>
          <span><span class="legend-dot" style="background:#26de81"></span>Cheap (p30)</span>
          <span><span class="legend-dot" style="background:#fed330"></span>Normal</span>
          <span><span class="legend-dot" style="background:#ff4b4b"></span>Expensive</span>
        </div>
        """, unsafe_allow_html=True)

        # Use ref_prices (today_p96 when available, else rolling) for the chart
        chart_data = ref_prices if ref_prices else prices
        n_pts = len(chart_data)
        x_times = [f"{(i * 15) // 60:02d}:{(i * 15) % 60:02d}" for i in range(n_pts)]
        hour_ticks = [f"{h:02d}:00" for h in range(0, 24, 2)]  # every 2 hours

        fig = go.Figure()
        fig.add_trace(go.Scatter(
            x=x_times, y=list(chart_data),
            mode='lines',
            line=dict(color='#00e5ff', width=1.5),
            fill='tozeroy', fillcolor='rgba(0,229,255,0.05)',
            hovertemplate='%{x}  %{y:.1f} €/MWh<extra></extra>'
        ))
        # Vertical line at current interval
        if cur_idx < n_pts:
            fig.add_vline(x=x_times[cur_idx], line_width=1, line_dash='dash', line_color='#fed330')
        fig.update_layout(
            height=180,
            paper_bgcolor='#0d1520', plot_bgcolor='#0d1520',
            margin=dict(l=50, r=10, t=5, b=40),
            xaxis=dict(
                tickvals=hour_ticks, ticktext=hour_ticks,
                tickfont=dict(color='#4a6070', size=9),
                gridcolor='#1e2d3d', linecolor='#1e2d3d',
                title=None
            ),
            yaxis=dict(
                title='€/MWh',
                tickfont=dict(color='#4a6070', size=9),
                title_font=dict(color='#4a6070', size=9),
                gridcolor='#1e2d3d', linecolor='#1e2d3d',
                zeroline=False
            ),
            showlegend=False
        )
        st.plotly_chart(fig, use_container_width=True, config={'displayModeBar': False})

        # percentile reference metrics
        m1, m2, m3 = st.columns(3)
        m1.metric("p10 — Very Cheap", f"{p10:.1f} €")
        m2.metric("p30 — Cheap",      f"{p30:.1f} €")
        m3.metric("p70 — Normal",     f"{p70:.1f} €")
    else:
        st.info("⏳ Waiting for energy price data…")

    # ── 96-slot day heatmap ──
    st.markdown('<div class="section-hdr">Today\'s Price Map — 96 × 15-min Intervals</div>', unsafe_allow_html=True)

    # TODAY prices only exist after the first midnight shift (producer sends TOMORROW only).
    # Never fall back to rolling prices — that makes today and tomorrow look identical.
    for map_label, map_prices in [("TODAY", today_p96), ("TOMORROW", tomorrow_p96)]:
        if not map_prices:
            # if map_label == "TODAY":
            #     st.caption("TODAY: prices available after midnight — showing tomorrow's preview below")
            # else:
            #     st.caption("")
            continue
        slots_html = f'<div style="font-family:Share Tech Mono,monospace;font-size:0.65rem;color:#4a6070;margin-bottom:3px">{map_label}</div>'
        slots_html += '<div class="sched-row">'
        for t, p in enumerate(map_prices):
            col_hex = price_color(p, p10, p30, p70)
            border  = ' outline: 1px solid #00e5ff;' if (map_label == "TODAY" and t == cur_idx) else ''
            title   = f"{(t*15)//60:02d}:{(t*15)%60:02d}  {p:.1f}€"
            slots_html += f'<div class="sched-slot" style="background:{col_hex};{border}" title="{title}"></div>'
        slots_html += '</div>'
        st.markdown(slots_html, unsafe_allow_html=True)

    # ── Per-car charging schedule ──
    st.markdown('<div class="section-hdr">Planned Charging Schedules (Flink Output)</div>', unsafe_allow_html=True)

    _today_smart  = st.session_state['today_charging_cost']
    _total_smart  = st.session_state['total_charging_cost']
    _today_test   = st.session_state['today_test_cost']
    _total_test   = st.session_state['total_test_cost']
    _saved_today  = _today_test  - _today_smart
    _saved_total  = _total_test  - _total_smart
    _dr_today_r   = st.session_state['today_driven_real']
    _dr_total_r   = st.session_state['total_driven_real']
    _dr_today_t   = st.session_state['today_driven_test']
    _dr_total_t   = st.session_state['total_driven_test']
    st.markdown(
        f'<div style="font-family:Share Tech Mono,monospace;font-size:0.75rem;color:#cfd8dc;margin-bottom:6px">'
        f' Smart — Today: <span style="color:#00e5ff">€{_today_smart:.4f}</span>'
        f'&nbsp; Total: <span style="color:#00e5ff">€{_total_smart:.4f}</span>'
        f'&nbsp; Driven: <span style="color:#00e5ff">{_dr_today_r:.1f}h today / {_dr_total_r:.1f}h total</span>'
        f'&nbsp;&nbsp;&nbsp;&nbsp;<br>'
        f' Naive — Today: <span style="color:#ff5252">€{_today_test:.4f}</span>'
        f'&nbsp; Total: <span style="color:#ff5252">€{_total_test:.4f}</span>'
        f'&nbsp; Driven: <span style="color:#ff5252">{_dr_today_t:.1f}h today / {_dr_total_t:.1f}h total</span>'
        f'&nbsp;&nbsp;&nbsp;&nbsp; <br>'
        f' Saved — Today: <span style="color:#26de81">€{_saved_today:.4f}</span>'
        f'&nbsp; Total: <span style="color:#26de81">€{_saved_total:.4f}</span>'
        f'</div>',
        unsafe_allow_html=True
    )

    # ── Comparison charts: cost savings + driving hours ──
    _chart_col1, _chart_col2 = st.columns(2)

    with _chart_col1:
        _cost_fig = go.Figure()
        _cost_fig.add_trace(go.Bar(
            name='Smart',
            x=['Today', 'Total'],
            y=[_today_smart, _total_smart],
            marker_color='#00e5ff',
            marker_line_width=0,
        ))
        _cost_fig.add_trace(go.Bar(
            name='Naive',
            x=['Today', 'Total'],
            y=[_today_test, _total_test],
            marker_color='#ff5252',
            marker_line_width=0,
        ))
        _cost_fig.add_trace(go.Bar(
            name='Saved',
            x=['Today', 'Total'],
            y=[max(0.0, _saved_today), max(0.0, _saved_total)],
            marker_color='#26de81',
            marker_line_width=0,
        ))
        _cost_fig.update_layout(
            title=dict(text='Charging Cost (€)', font=dict(color='#8ba3bc', size=11), x=0),
            barmode='group',
            height=200,
            paper_bgcolor='#0d1520', plot_bgcolor='#0d1520',
            margin=dict(l=40, r=10, t=30, b=30),
            legend=dict(font=dict(color='#4a6070', size=9), bgcolor='rgba(0,0,0,0)', orientation='h', y=-0.25),
            xaxis=dict(tickfont=dict(color='#4a6070', size=9), gridcolor='#1e2d3d', linecolor='#1e2d3d'),
            yaxis=dict(tickfont=dict(color='#4a6070', size=9), gridcolor='#1e2d3d', linecolor='#1e2d3d',
                       tickprefix='€', zeroline=False),
        )
        st.plotly_chart(_cost_fig, use_container_width=True, config={'displayModeBar': False})

    with _chart_col2:
        _drive_fig = go.Figure()
        _drive_fig.add_trace(go.Bar(
            name='Smart',
            x=['Today', 'Total'],
            y=[_dr_today_r, _dr_total_r],
            marker_color='#00e5ff',
            marker_line_width=0,
        ))
        _drive_fig.add_trace(go.Bar(
            name='Naive',
            x=['Today', 'Total'],
            y=[_dr_today_t, _dr_total_t],
            marker_color='#ff5252',
            marker_line_width=0,
        ))
        _drive_fig.update_layout(
            title=dict(text='Driving Hours (sim-h)', font=dict(color='#8ba3bc', size=11), x=0),
            barmode='group',
            height=200,
            paper_bgcolor='#0d1520', plot_bgcolor='#0d1520',
            margin=dict(l=40, r=10, t=30, b=30),
            legend=dict(font=dict(color='#4a6070', size=9), bgcolor='rgba(0,0,0,0)', orientation='h', y=-0.25),
            xaxis=dict(tickfont=dict(color='#4a6070', size=9), gridcolor='#1e2d3d', linecolor='#1e2d3d'),
            yaxis=dict(tickfont=dict(color='#4a6070', size=9), gridcolor='#1e2d3d', linecolor='#1e2d3d',
                       ticksuffix='h', zeroline=False),
        )
        st.plotly_chart(_drive_fig, use_container_width=True, config={'displayModeBar': False})

    plans = st.session_state['charging_plans']
    if plans:
        pre_midnight = len(today_p96) == 0
        if pre_midnight:
            day_rows = [("TOMORROW — pre-planned", tomorrow_p96, 0)]
        else:
            day_rows = [("TODAY", today_p96, 0), ("TOMORROW", tomorrow_p96, 96)]

        real_plans = {k: v for k, v in sorted(plans.items()) if not k.endswith('_test')}

        # ── Hour ruler (shared across all day rows) ──
        SLOT_W = 5   # px per 15-min slot
        CAR_LBL_W = 60  # px for car label column
        ruler_html = (f'<div style="display:flex;margin-left:{CAR_LBL_W}px;margin-bottom:2px">')
        for h in range(0, 24, 3):
            ruler_html += (
                f'<div style="width:{SLOT_W * 12}px;flex-shrink:0;font-family:JetBrains Mono,monospace;'
                f'font-size:0.55rem;color:#2d4a62;text-align:left">{h:02d}:00</div>'
            )
        ruler_html += '</div>'

        for day_label, price_buf, offset in day_rows:
            if not price_buf and offset >= 96:
                continue  # skip tomorrow if no data yet

            sched_html = f'''
            <div style="background:#080f1a;border:1px solid #0f1f30;border-radius:10px;
                        padding:0.7rem 0.8rem;margin-bottom:0.6rem">
              <div style="font-family:JetBrains Mono,monospace;font-size:0.62rem;color:#00e5ff;
                          letter-spacing:0.12em;margin-bottom:6px;text-transform:uppercase">{day_label}</div>
              {ruler_html}
            '''

            for car_id, planned_slots in real_plans.items():
                planned_set = set(planned_slots)
                label = car_id.upper().replace('_', ' ')
                sched_html += (
                    f'<div style="display:flex;align-items:center;margin-bottom:3px">'
                    f'<div style="width:{CAR_LBL_W}px;flex-shrink:0;font-family:JetBrains Mono,monospace;'
                    f'font-size:0.6rem;color:#4a6070;white-space:nowrap;overflow:hidden;'
                    f'text-overflow:ellipsis">{label}</div>'
                    f'<div style="display:flex;gap:1px">'
                )
                for t in range(96):
                    abs_t   = t + offset
                    is_past = (offset == 0 and t < cur_idx)
                    is_now  = (offset == 0 and t == cur_idx)
                    if abs_t in planned_set:
                        bg     = '#004455' if is_past else '#00e5ff'
                        radius = '2px'
                    elif t < len(price_buf) and price_buf[t] <= p30:
                        bg     = '#26de8128'
                        radius = '2px'
                    else:
                        bg     = '#111c2b'
                        radius = '2px'
                    outline = 'outline:1px solid #ffffff55;' if is_now else ''
                    sched_html += (
                        f'<div style="width:{SLOT_W}px;height:10px;background:{bg};'
                        f'border-radius:{radius};flex-shrink:0;{outline}"></div>'
                    )
                sched_html += '</div></div>'

            sched_html += '</div>'
            st.markdown(sched_html, unsafe_allow_html=True)

        st.markdown("""
        <div class="legend-row" style="margin-top:0.2rem">
          <span><span class="legend-dot" style="background:#00e5ff"></span>Planned</span>
          <span><span class="legend-dot" style="background:#004455"></span>Charged (past)</span>
          <span><span class="legend-dot" style="background:#26de81;opacity:0.5"></span>Cheap window</span>
          <span><span class="legend-dot" style="background:#111c2b"></span>No action</span>
        </div>
        """, unsafe_allow_html=True)
    else:
        cmds = st.session_state['topic_counts']['charging_commands']
        if cmds == 0:
            st.warning("No charging commands received yet — is the Flink job running?  (CMDS received: 0)")
        else:
            st.caption(f"Commands received ({cmds}) but no car_id found — check Flink output format")


# ═══════════════════════════════════════════════
#  RIGHT — Car Battery Cards
# ═══════════════════════════════════════════════
with right_col:
    st.markdown('<div class="section-hdr">Charging Stations</div>', unsafe_allow_html=True)

    STATION_CAPACITY = 5
    cars = st.session_state['cars']
    # Assign currently-charging real cars to station slots (sorted for stability)
    _charging_real = sorted(
        [cid for cid, c in cars.items() if not cid.endswith('_test') and c.get('plugged_in', False)]
    )
    _station_html = '<div style="display:flex;gap:8px;flex-wrap:wrap;margin-bottom:1rem">'
    for _slot in range(STATION_CAPACITY):
        if _slot < len(_charging_real):
            _cid   = _charging_real[_slot]
            _soc   = float(cars[_cid].get('current_soc', 0.0)) * 100
            _label = _cid.upper().replace('_', ' ')
            _station_html += f'''
            <div style="flex:1;min-width:70px;background:#080f1a;border:1px solid #00e5ff40;
                        border-radius:10px;padding:0.6rem 0.5rem;text-align:center;
                        box-shadow:0 0 10px #00e5ff18;animation:pulse-border 2.5s ease-in-out infinite">
              <div style="font-size:1.3rem;margin-bottom:2px">⚡</div>
              <div style="font-family:JetBrains Mono,monospace;font-size:0.6rem;color:#00e5ff;
                          letter-spacing:0.08em;margin-bottom:4px">{_label}</div>
              <div style="font-family:JetBrains Mono,monospace;font-size:1rem;color:#e2eaf4;
                          font-weight:500">{_soc:.0f}<span style="font-size:0.65rem;color:#4a6070">%</span></div>
              <div style="height:4px;background:#0a1525;border-radius:2px;margin-top:5px;overflow:hidden">
                <div style="height:100%;width:{_soc:.0f}%;background:#00e5ff;border-radius:2px;
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

    st.markdown('<div class="section-hdr">Fleet Status</div>', unsafe_allow_html=True)

    if cars:
        real_cars_map = {k: v for k, v in sorted(cars.items()) if not k.endswith('_test')}
        test_cars_map = {k: v for k, v in sorted(cars.items()) if k.endswith('_test')}

        for group_cars, group_label, group_color in [
            (real_cars_map, '⚡ Smart Scheduler', '#00e5ff'),
            (test_cars_map,  ' Naive (no schedule)', '#ff5252'),
        ]:
            if not group_cars:
                continue
            st.markdown(
                f'<div style="font-family:Share Tech Mono,monospace;font-size:0.65rem;'
                f'color:{group_color};margin:8px 0 4px;border-bottom:1px solid {group_color}33;'
                f'padding-bottom:3px">{group_label}</div>',
                unsafe_allow_html=True)

            all_cards_html = ""
            for car_id, car in group_cars.items():
                try:
                    soc        = float(car.get('current_soc', 0.0))
                    soc_pct    = round(soc * 100, 1)
                    plugged    = bool(car.get('plugged_in', False))
                    priority   = str(car.get('priority', '-'))
                    kwh        = float(car.get('current_battery_kwh', 0.0))
                    fill_color = soc_color(soc)
                    is_critical = soc < 0.15
                    batt_cls   = 'charging' if plugged else ''

                    if plugged:
                        badge_html = '&#9889; CHG'
                        badge_cls  = 'badge-charging'
                    elif is_critical:
                        badge_html = '&#9888; CRIT'
                        badge_cls  = 'badge-critical'
                    else:
                        badge_html = '&#9679; IDLE'
                        badge_cls  = 'badge-idle'

                    # Mini plan dots (next 24 slots = 6h)
                    slots_html = ""
                    if not car_id.endswith('_test'):
                        plan_set = set(st.session_state['charging_plans'].get(car_id, []))
                        card_price_buf = today_p96 if today_p96 else tomorrow_p96
                        for t in range(cur_idx, min(cur_idx + 24, 96)):
                            if t in plan_set:
                                bg = '#00e5ff'
                            elif t < len(card_price_buf) and card_price_buf[t] <= p30:
                                bg = '#26de8140'
                            else:
                                bg = '#1e2d3d'
                            slots_html += f'<div style="width:5px;height:5px;border-radius:1px;background:{bg};flex-shrink:0"></div>'

                    label = car_id.upper().replace("_", " ")
                    border_col = '#00e5ff30' if plugged else ('#ff4b4b30' if is_critical else '#0f1f30')
                    row_anim   = 'animation:pulse-border 2.5s ease-in-out infinite;' if plugged else (
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
                          f'color:#4a6070;width:36px;flex-shrink:0;text-align:center">P{priority}</div>'
                          # Plan dots
                          f'<div style="display:flex;gap:2px;flex-shrink:0">{slots_html}</div>'
                          # Badge
                          f'<div style="flex-shrink:0"><span class="badge {badge_cls}" '
                          f'style="font-size:0.55rem;padding:0.15rem 0.45rem">{badge_html}</span></div>'
                        f'</div>'
                    )
                except Exception as e:
                    all_cards_html += f'<div style="color:#ff4b4b;font-size:0.8rem;padding:0.5rem">Error [{car_id}]: {e}</div>'

            st.markdown(all_cards_html, unsafe_allow_html=True)
    else:
        st.info("Waiting for car telemetry...")
        st.caption(f"Consumer ready: {st.session_state['kafka_ready']} | Messages: {st.session_state['total_messages']}")

# ── Auto-refresh ───────────────────────────────────────────────────────────────
time.sleep(1)
st.rerun()