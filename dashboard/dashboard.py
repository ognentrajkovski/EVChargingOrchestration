import os
import sys
import time
import json
import uuid

import streamlit as st
import pandas as pd
import numpy as np
import plotly.graph_objects as go
from kafka import KafkaConsumer

# ── Logging ────────────────────────────────────────────────────────────────────
_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__))) if '__file__' in dir() else '.'
sys.path.insert(0, _root)
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
STATION_CAPACITY     = 5
BASE_PRICE           = 50.0

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
    'reservation_counts':      {},      # str(slot_idx) → count of reservations
    'projected_prices':        [],      # list of 96 floats
    'hourly_profile':          {},      # str(hour) → {peak_count, total_count}
    'instant_price':           None,   # last instantaneous component price
    'hist_price':              None,   # last historical component price
    'compound_boost':          False,  # whether compound boost is active
    'price_day_snapshots':     [],      # list of {'label': str, 'prices': [96 floats]}
    'last_snapshot_day':       -1,      # last day_idx we snapshotted
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
                        price    = data.get('current_price')
                        chargers = data.get('active_chargers', 0)
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

                            # Capture per-component breakdown from energy producer
                            if 'hourly_profile' in data:
                                st.session_state['hourly_profile'] = data['hourly_profile']
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
                        continue

                    car_id = data.get('car_id')
                    action = data.get('action', '')
                    reason = data.get('reason', '')
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

# ── Clock strip ────────────────────────────────────────────────────────────────
price_col     = ('#ff4b4b' if current_price is not None and current_price > BASE_PRICE * 1.4
                 else '#26de81' if current_price is not None and current_price <= BASE_PRICE * 1.1
                 else '#fed330')
charger_col   = occupancy_color(current_chargers)
price_display = f'{current_price:.1f} <span style="font-size:1rem">€</span>' if current_price is not None else '—'

st.markdown(f"""
<div class="clock-strip">
  <div class="clock-cell">
    <div class="clock-label">Live Price</div>
    <div class="clock-val" style="color:{price_col}">{price_display}</div>
  </div>
  <div class="clock-cell">
    <div class="clock-label">Active Chargers</div>
    <div class="clock-val" style="color:{charger_col}">{current_chargers} <span style="font-size:1rem;color:#1a3048">/ {STATION_CAPACITY}</span></div>
  </div>
  <div class="clock-cell">
    <div class="clock-label">Cars Plugged In</div>
    <div class="clock-val">{charging_count}</div>
  </div>
  <div class="clock-cell">
    <div class="clock-label">Base Price</div>
    <div class="clock-val accent">{BASE_PRICE:.0f} <span style="font-size:1rem">€</span></div>
  </div>
  <div class="clock-cell">
    <div class="clock-label">Peak Multiplier</div>
    <div class="clock-val">×{PRICE_STATES[-1][2]:.1f}</div>
  </div>
</div>
""", unsafe_allow_html=True)

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

    # Current price state badge — always visible
    _off_p, _norm_p, _peak_p = [BASE_PRICE * m for _, m, *_ in [(s[0], s[2]) for s in PRICE_STATES]]
    _cr = (current_chargers or 0) / STATION_CAPACITY
    if _cr > PRICE_STATES[1][1]:
        _state_lbl, _state_col, _state_price = 'PEAK',     '#ff4b4b', _peak_p
    elif _cr > PRICE_STATES[0][1]:
        _state_lbl, _state_col, _state_price = 'NORMAL',   '#fed330', _norm_p
    else:
        _state_lbl, _state_col, _state_price = 'OFF-PEAK', '#26de81', _off_p

    _cur_p_disp  = f'{current_price:.1f}' if current_price is not None else f'{_state_price:.1f}'
    _inst_p      = st.session_state.get('instant_price')
    _hist_p      = st.session_state.get('hist_price')
    _compound    = st.session_state.get('compound_boost', False)
    _boost_badge = (
        '<span style="background:#ff4b4b;color:#fff;font-size:0.55rem;padding:2px 6px;'
        'border-radius:4px;letter-spacing:0.06em;margin-left:8px">⚡ COMPOUND PEAK</span>'
        if _compound else ''
    )
    _breakdown = ''
    if _inst_p is not None and _hist_p is not None:
        _breakdown = (
            f'<div style="font-family:JetBrains Mono,monospace;font-size:0.6rem;color:#4a6070;margin-top:3px">'
            f'Instant <span style="color:#00e5ff">{_inst_p:.0f}€</span> ×60%'
            f' &nbsp;+&nbsp; Learned <span style="color:#7c5cbf">{_hist_p:.0f}€</span> ×40%'
            f'{" &nbsp;+&nbsp; <span style=\'color:#ff4b4b\'>×1.3 boost</span>" if _compound else ""}'
            f' &nbsp;= <span style="color:{_state_col};font-weight:700">{_cur_p_disp}€</span>'
            f'</div>'
        )
    st.markdown(f'''
    <div style="padding:0.5rem 0.8rem;background:{_state_col}12;border:1px solid {_state_col}40;
                border-radius:10px;margin-bottom:0.4rem">
      <div style="display:flex;align-items:center;gap:16px">
        <div style="font-family:JetBrains Mono,monospace">
          <div style="font-size:0.6rem;color:#4a6070;letter-spacing:0.1em">CURRENT STATE</div>
          <div style="font-size:1.2rem;font-weight:700;color:{_state_col};letter-spacing:0.08em">{_state_lbl}{_boost_badge}</div>
        </div>
        <div style="width:1px;height:36px;background:{_state_col}30"></div>
        <div style="font-family:JetBrains Mono,monospace">
          <div style="font-size:0.6rem;color:#4a6070;letter-spacing:0.1em">PRICE NOW</div>
          <div style="font-size:1.2rem;font-weight:700;color:{_state_col}">{_cur_p_disp} <span style="font-size:0.7rem">€/MWh</span></div>
        </div>
        <div style="width:1px;height:36px;background:{_state_col}30"></div>
        <div style="font-family:JetBrains Mono,monospace">
          <div style="font-size:0.6rem;color:#4a6070;letter-spacing:0.1em">CHARGERS BUSY</div>
          <div style="font-size:1.2rem;font-weight:700;color:{_state_col}">{current_chargers or 0}<span style="font-size:0.7rem;color:#4a6070"> / {STATION_CAPACITY}</span></div>
        </div>
        <div style="flex:1"></div>
        <div style="font-family:JetBrains Mono,monospace;font-size:0.6rem;color:#4a6070;text-align:right">
          OFF-PEAK <span style="color:#26de81">{BASE_PRICE*PRICE_STATES[0][2]:.0f}€</span> &nbsp;·&nbsp;
          NORMAL <span style="color:#fed330">{BASE_PRICE*PRICE_STATES[1][2]:.0f}€</span> &nbsp;·&nbsp;
          PEAK <span style="color:#ff4b4b">{BASE_PRICE*PRICE_STATES[2][2]:.0f}€</span> &nbsp;·&nbsp;
          COMPOUND <span style="color:#ff4b4b">up to {BASE_PRICE*PRICE_STATES[2][2]*1.3:.0f}€</span>
        </div>
      </div>
      {_breakdown}
    </div>
    ''', unsafe_allow_html=True)

    if dynamic_prices:
        n_pts    = len(dynamic_prices)
        x_vals   = list(range(-n_pts + 1, 1))
        t_labels = list(st.session_state['time_labels'])[-n_pts:]
        t_labels = t_labels + [''] * (n_pts - len(t_labels))

        # Determine color of each point by its price state
        def _price_color(p):
            if p >= BASE_PRICE * PRICE_STATES[2][2] * 0.9:   return '#ff4b4b'
            if p >= BASE_PRICE * PRICE_STATES[1][2] * 0.9:   return '#fed330'
            return '#26de81'

        pt_colors = [_price_color(p) for p in dynamic_prices]

        fig = go.Figure()

        # Colored background bands for each price zone
        fig.add_hrect(y0=0,               y1=BASE_PRICE*PRICE_STATES[0][2], fillcolor='rgba(38,222,129,0.04)', line_width=0)
        fig.add_hrect(y0=BASE_PRICE*PRICE_STATES[0][2], y1=BASE_PRICE*PRICE_STATES[1][2], fillcolor='rgba(254,211,48,0.04)',  line_width=0)
        fig.add_hrect(y0=BASE_PRICE*PRICE_STATES[1][2], y1=BASE_PRICE*PRICE_STATES[2][2]*1.2, fillcolor='rgba(255,75,75,0.04)',   line_width=0)

        # Price line — colored segments by state
        # Draw as separate segments so each can have its own color
        prev_col = pt_colors[0]
        seg_x, seg_y = [x_vals[0]], [dynamic_prices[0]]
        for i in range(1, n_pts):
            seg_x.append(x_vals[i])
            seg_y.append(dynamic_prices[i])
            if pt_colors[i] != prev_col or i == n_pts - 1:
                fig.add_trace(go.Scatter(
                    x=seg_x, y=seg_y, mode='lines',
                    line=dict(color=prev_col, width=2.5),
                    showlegend=False, hoverinfo='skip',
                ))
                seg_x, seg_y = [x_vals[i]], [dynamic_prices[i]]
                prev_col = pt_colors[i]

        # Invisible scatter for hover tooltips
        fig.add_trace(go.Scatter(
            x=x_vals, y=list(dynamic_prices),
            mode='markers', marker=dict(size=4, color=pt_colors, opacity=0.8),
            customdata=t_labels,
            hovertemplate='%{customdata}<br><b>%{y:.1f} €/MWh</b><extra></extra>',
            showlegend=False,
        ))

        # Threshold lines
        for lbl, _, mult, col in PRICE_STATES:
            fig.add_hline(
                y=BASE_PRICE * mult, line_dash='dot', line_color=col, line_width=1,
                annotation_text=f'{lbl}  {BASE_PRICE*mult:.0f}€',
                annotation_font_color=col, annotation_font_size=9,
                annotation_position='right',
            )

        y_max = max(max(dynamic_prices) * 1.15, BASE_PRICE * PRICE_STATES[2][2] * 1.1)
        fig.update_layout(
            height=220,
            paper_bgcolor='#0d1520', plot_bgcolor='#0d1520',
            margin=dict(l=50, r=95, t=10, b=35),
            xaxis=dict(
                tickmode='array', tickvals=x_vals[::20], ticktext=t_labels[::20],
                tickfont=dict(color='#4a6070', size=9),
                gridcolor='#1e2d3d', linecolor='#1e2d3d', zeroline=False, title=None,
            ),
            yaxis=dict(
                title='€/MWh', tickfont=dict(color='#4a6070', size=9),
                title_font=dict(color='#4a6070', size=9),
                gridcolor='#1e2d3d', linecolor='#1e2d3d', zeroline=False,
                range=[0, y_max],
            ),
            showlegend=False,
        )
        st.plotly_chart(fig, use_container_width=True, config={'displayModeBar': False})
    else:
        st.info("⏳ Waiting for dynamic price data…")

    # ── Active charger count history ──────────────────────────────────────────
    if active_chargers_hist and len(active_chargers_hist) > 1:
        n_pts   = len(active_chargers_hist)
        x_vals  = list(range(-n_pts + 1, 1))
        t2_lbls = list(st.session_state['time_labels'])[-n_pts:]
        t2_lbls = t2_lbls + [''] * (n_pts - len(t2_lbls))
        fig2    = go.Figure()
        fig2.add_trace(go.Scatter(
            x=x_vals, y=list(active_chargers_hist),
            mode='lines',
            line=dict(color='#7c5cbf', width=1.5),
            fill='tozeroy', fillcolor='rgba(124,92,191,0.06)',
            customdata=t2_lbls,
            hovertemplate='Time: %{customdata}<br>%{y} chargers active<extra></extra>',
        ))
        fig2.update_layout(
            height=100,
            paper_bgcolor='#0d1520', plot_bgcolor='#0d1520',
            margin=dict(l=50, r=90, t=5, b=30),
            xaxis=dict(
                tickmode='array', tickvals=x_vals[::20], ticktext=t2_lbls[::20],
                tickfont=dict(color='#4a6070', size=9), gridcolor='#1e2d3d', linecolor='#1e2d3d', zeroline=False, title=None
            ),
            yaxis=dict(
                title='Chargers',
                tickfont=dict(color='#4a6070', size=9),
                title_font=dict(color='#4a6070', size=9),
                gridcolor='#1e2d3d', linecolor='#1e2d3d',
                zeroline=False,
                range=[0, STATION_CAPACITY + 0.5],
                dtick=1,
            ),
            showlegend=False,
        )
        st.plotly_chart(fig2, use_container_width=True, config={'displayModeBar': False})

    # ── Price ladder ──────────────────────────────────────────────────────────
    st.markdown('<div class="section-hdr">Price Ladder — Occupancy Impact</div>', unsafe_allow_html=True)

    current_ratio   = (current_chargers or 0) / STATION_CAPACITY
    peak_price      = BASE_PRICE * PRICE_STATES[-1][2]   # 90 EUR/MWh

    ladder_html = '<div style="display:flex;flex-direction:column;gap:4px">'
    for state_label, upper, mult, col in PRICE_STATES:
        ref_p     = BASE_PRICE * mult
        is_active = (
            (state_label == 'Off-peak' and current_ratio <= PRICE_STATES[0][1]) or
            (state_label == 'Normal'   and PRICE_STATES[0][1] < current_ratio <= PRICE_STATES[1][1]) or
            (state_label == 'Peak'     and current_ratio > PRICE_STATES[1][1])
        )
        fill_pct  = int(ref_p / peak_price * 100)
        outline   = f'border:1px solid {col}60;' if is_active else 'border:1px solid #0f1f30;'
        bg_active = f'background:#0d1520;{outline}' if is_active else 'background:#080f1a;border:1px solid #0f1f30;'
        occ_range = ('0–30%' if state_label == 'Off-peak' else
                     '31–60%' if state_label == 'Normal' else '61–100%')
        row_label = f'{"▶ " if is_active else ""}{state_label}  ({occ_range} occupied)'
        shadow    = f'box-shadow:0 0 12px {col}30;' if is_active else ''

        ladder_html += f'''
        <div style="{bg_active}{shadow}border-radius:8px;padding:0.45rem 0.8rem;display:flex;align-items:center;gap:12px;transition:all 0.3s ease">
          <div style="font-family:JetBrains Mono,monospace;font-size:0.65rem;color:{"#e2eaf4" if is_active else "#4a6070"};width:200px;flex-shrink:0">{row_label}</div>
          <div style="flex:1;height:6px;background:#0a1525;border-radius:3px;overflow:hidden">
            <div style="width:{fill_pct}%;height:100%;background:{col};border-radius:3px;{"animation:batt-shimmer 2s linear infinite;background-size:200% auto;" if is_active else ""}"></div>
          </div>
          <div style="font-family:JetBrains Mono,monospace;font-size:0.75rem;color:{col};width:80px;text-align:right;flex-shrink:0">{ref_p:.1f} €/MWh</div>
        </div>'''
    ladder_html += '</div>'
    st.markdown(ladder_html, unsafe_allow_html=True)

    # ── Full-day price profile (multi-day) ────────────────────────────────────
    st.markdown('<div class="section-hdr">Daily Price Profile — All Day</div>', unsafe_allow_html=True)

    projected = st.session_state.get('projected_prices', [])
    day_snaps = st.session_state.get('price_day_snapshots', [])

    # Build x-axis: slot 0..95 → "HH:MM"
    slot_labels = [f"{(s*15)//60:02d}:{(s*15)%60:02d}" for s in range(96)]

    fig_day = go.Figure()
    day_colors = ['#7c5cbf', '#ff9800', '#26de81']   # purple, orange, green — all visible on dark bg
    for i, snap in enumerate(day_snaps):
        fig_day.add_trace(go.Scatter(
            x=slot_labels, y=snap['prices'],
            mode='lines',
            line=dict(color=day_colors[i % len(day_colors)], width=1.5, dash='dot'),
            name=snap['label'],
            opacity=0.8,
            hovertemplate='%{x}<br>%{y:.1f} €/MWh<extra></extra>',
        ))
    if projected and len(projected) == 96:
        fig_day.add_trace(go.Scatter(
            x=slot_labels, y=projected,
            mode='lines',
            line=dict(color='#00e5ff', width=2),
            fill='tozeroy', fillcolor='rgba(0,229,255,0.04)',
            name='Today',
            hovertemplate='%{x}<br>%{y:.1f} €/MWh<extra></extra>',
        ))
    elif not projected and not day_snaps:
        # Show static reference when no live data yet
        static_y = []
        for s in range(96):
            h = (s * 15) // 60
            # Simulate a typical day: off-peak night, peak morning/evening
            if 7 <= h <= 9 or 17 <= h <= 20:
                static_y.append(BASE_PRICE * 1.8)
            elif 10 <= h <= 16:
                static_y.append(BASE_PRICE * 1.0)
            else:
                static_y.append(BASE_PRICE * 0.6)
        fig_day.add_trace(go.Scatter(
            x=slot_labels, y=static_y,
            mode='lines',
            line=dict(color='#2d4a62', width=1.5, dash='dot'),
            name='Typical (demo)',
            hovertemplate='%{x}<br>%{y:.1f} €/MWh<extra></extra>',
        ))

    # Add price state bands as horizontal lines
    for lbl, _, mult, col in PRICE_STATES:
        fig_day.add_hline(
            y=BASE_PRICE * mult, line_dash='dot', line_color=col, line_width=0.5,
            annotation_text=f'{lbl}', annotation_font_color=col, annotation_font_size=8,
            annotation_position='right',
        )
    # Mark current time slot
    cur_idx = st.session_state.get('last_snapshot_day', -1)
    if 0 <= cur_idx < 96:
        fig_day.add_vline(
            x=slot_labels[cur_idx], line_dash='solid', line_color='rgba(255,255,255,0.18)', line_width=1,
        )
    fig_day.update_layout(
        height=210,
        paper_bgcolor='#0d1520', plot_bgcolor='#0d1520',
        margin=dict(l=50, r=90, t=10, b=35),
        xaxis=dict(
            tickmode='array',
            tickvals=slot_labels[::8],   # every 2 hours
            ticktext=slot_labels[::8],
            tickfont=dict(color='#4a6070', size=8),
            gridcolor='#1e2d3d', linecolor='#1e2d3d', zeroline=False, title=None,
        ),
        yaxis=dict(
            title='€/MWh', tickfont=dict(color='#4a6070', size=9),
            title_font=dict(color='#4a6070', size=9),
            gridcolor='#1e2d3d', linecolor='#1e2d3d', zeroline=False,
            range=[0, BASE_PRICE * 2.2],
        ),
        legend=dict(
            font=dict(color='#4a6070', size=8),
            bgcolor='rgba(0,0,0,0)', x=0.01, y=0.99,
        ),
    )
    st.plotly_chart(fig_day, use_container_width=True, config={'displayModeBar': False})

    # ── Traffic → Price Impact ─────────────────────────────────────────────────
    st.markdown('<div class="section-hdr">Traffic → Price Impact</div>', unsafe_allow_html=True)

    charger_counts = list(range(STATION_CAPACITY + 1))
    off_m, norm_m, peak_m = PRICE_STATES[0][2], PRICE_STATES[1][2], PRICE_STATES[2][2]

    def _inst_price(n):
        ratio = n / STATION_CAPACITY
        for _, upper, mult, _ in PRICE_STATES:
            if ratio <= upper:
                return BASE_PRICE * mult
        return BASE_PRICE * PRICE_STATES[-1][2]

    # Blended price with different historic profiles
    def _blended(n, peak_freq):
        inst  = _inst_price(n)
        hist  = BASE_PRICE * (off_m + (peak_m - off_m) * peak_freq)
        return inst * 0.6 + hist * 0.4

    y_instant  = [_inst_price(n) for n in charger_counts]
    y_low_hist = [_blended(n, 0.1) for n in charger_counts]   # quiet history
    y_high_hist= [_blended(n, 0.8) for n in charger_counts]   # busy history

    # Get current blended price data from live hourly profile if available
    profile = st.session_state.get('hourly_profile', {})
    cur_hour = (st.session_state.get('last_snapshot_day', 48) * 15) // 60
    cur_prof = profile.get(str(cur_hour), {})
    cur_total= cur_prof.get('total_count', 0)
    cur_peak = cur_prof.get('peak_count', 0)
    cur_freq = (cur_peak / cur_total) if cur_total >= 4 else None
    y_live   = [_blended(n, cur_freq) for n in charger_counts] if cur_freq is not None else None

    fig_impact = go.Figure()
    fig_impact.add_trace(go.Scatter(
        x=charger_counts, y=y_high_hist,
        mode='lines', line=dict(color='#ff4b4b', width=1, dash='dot'),
        fill='tonexty', fillcolor='rgba(255,75,75,0.05)',
        name='Busy history (80% peak)', hovertemplate='%{x} chargers → %{y:.1f} €<extra></extra>',
    ))
    fig_impact.add_trace(go.Scatter(
        x=charger_counts, y=y_low_hist,
        mode='lines', line=dict(color='#26de81', width=1, dash='dot'),
        name='Quiet history (10% peak)', hovertemplate='%{x} chargers → %{y:.1f} €<extra></extra>',
    ))
    fig_impact.add_trace(go.Scatter(
        x=charger_counts, y=y_instant,
        mode='lines+markers',
        line=dict(color='#00e5ff', width=2.5),
        marker=dict(size=7, color='#00e5ff'),
        name='Instantaneous (no history)', hovertemplate='%{x} chargers → %{y:.1f} €<extra></extra>',
    ))
    if y_live:
        fig_impact.add_trace(go.Scatter(
            x=charger_counts, y=y_live,
            mode='lines+markers',
            line=dict(color='#fed330', width=2),
            marker=dict(size=6, color='#fed330'),
            name=f'Live blended (hour {cur_hour:02d}:xx)', hovertemplate='%{x} chargers → %{y:.1f} €<extra></extra>',
        ))
    # Mark current charger count
    cur_n = current_chargers or 0
    fig_impact.add_vline(
        x=cur_n, line_dash='solid', line_color='rgba(255,255,255,0.15)', line_width=1.5,
        annotation_text=f'Now ({cur_n})', annotation_font_color='#4a6070', annotation_font_size=8,
    )
    fig_impact.update_layout(
        height=210,
        paper_bgcolor='#0d1520', plot_bgcolor='#0d1520',
        margin=dict(l=50, r=10, t=10, b=35),
        xaxis=dict(
            title='Active chargers', tickfont=dict(color='#4a6070', size=9),
            title_font=dict(color='#4a6070', size=9),
            gridcolor='#1e2d3d', linecolor='#1e2d3d', zeroline=False,
            tickvals=charger_counts, dtick=1,
        ),
        yaxis=dict(
            title='€/MWh', tickfont=dict(color='#4a6070', size=9),
            title_font=dict(color='#4a6070', size=9),
            gridcolor='#1e2d3d', linecolor='#1e2d3d', zeroline=False,
            range=[0, BASE_PRICE * 2.2],
        ),
        legend=dict(
            font=dict(color='#4a6070', size=8),
            bgcolor='rgba(0,0,0,0)', x=0.4, y=0.99,
        ),
    )
    st.plotly_chart(fig_impact, use_container_width=True, config={'displayModeBar': False})

    # ── Learned Hourly Profile ────────────────────────────────────────────────
    st.markdown('<div class="section-hdr">What the Model Learned — Peak Frequency by Hour</div>', unsafe_allow_html=True)

    profile = st.session_state.get('hourly_profile', {})
    hours   = list(range(24))
    h_labels= [f'{h:02d}:00' for h in hours]

    peak_freqs  = []
    hist_prices = []
    bar_colors  = []
    obs_counts  = []
    off_m = PRICE_STATES[0][2]
    peak_m= PRICE_STATES[2][2]

    for h in hours:
        p   = profile.get(str(h), {})
        tot = p.get('total_count', 0)
        pk  = p.get('peak_count',  0)
        obs_counts.append(tot)
        if tot >= 4:
            freq = pk / tot
            hp   = BASE_PRICE * (off_m + (peak_m - off_m) * freq)
        else:
            freq = None
            hp   = None
        peak_freqs.append(freq)
        hist_prices.append(hp)
        if freq is None:
            bar_colors.append('#1a2d44')          # no data yet — dark placeholder
        elif freq >= 0.6:
            bar_colors.append('#ff4b4b')          # mostly peak
        elif freq >= 0.25:
            bar_colors.append('#fed330')          # mixed
        else:
            bar_colors.append('#26de81')          # mostly off-peak

    fig_profile = go.Figure()

    # Bar: peak frequency per hour
    fig_profile.add_trace(go.Bar(
        x=h_labels,
        y=[f if f is not None else 0 for f in peak_freqs],
        marker_color=bar_colors,
        name='Peak frequency',
        customdata=[(obs_counts[i], hist_prices[i] or 0) for i in hours],
        hovertemplate=(
            '<b>%{x}</b><br>'
            'Peak frequency: %{y:.0%}<br>'
            'Learned price: %{customdata[1]:.1f} €/MWh<br>'
            'Observations: %{customdata[0]}<extra></extra>'
        ),
    ))

    # Mark current hour (use numeric index — add_vline needs int on categorical axes)
    cur_h_idx = cur_hour % 24
    fig_profile.add_vline(
        x=cur_h_idx,
        line_dash='solid', line_color='rgba(255,255,255,0.3)', line_width=1.5,
        annotation_text='Now', annotation_font_color='#e2eaf4', annotation_font_size=9,
    )

    # Threshold lines at 25% and 60% peak frequency
    fig_profile.add_hline(y=0.6,  line_dash='dot', line_color='#ff4b4b', line_width=0.8,
                          annotation_text='→ Peak price zone',  annotation_font_color='#ff4b4b', annotation_font_size=8, annotation_position='right')
    fig_profile.add_hline(y=0.25, line_dash='dot', line_color='#fed330', line_width=0.8,
                          annotation_text='→ Normal price zone', annotation_font_color='#fed330', annotation_font_size=8, annotation_position='right')

    fig_profile.update_layout(
        height=200,
        paper_bgcolor='#0d1520', plot_bgcolor='#0d1520',
        margin=dict(l=50, r=110, t=10, b=40),
        xaxis=dict(
            tickfont=dict(color='#4a6070', size=8), tickangle=-45,
            gridcolor='#1e2d3d', linecolor='#1e2d3d', zeroline=False, title=None,
        ),
        yaxis=dict(
            title='% of time at peak', tickformat='.0%',
            tickfont=dict(color='#4a6070', size=9),
            title_font=dict(color='#4a6070', size=9),
            gridcolor='#1e2d3d', linecolor='#1e2d3d', zeroline=False,
            range=[0, 1.05],
        ),
        showlegend=False,
        bargap=0.15,
    )
    # Caption explaining what this shows
    st.plotly_chart(fig_profile, use_container_width=True, config={'displayModeBar': False})
    st.markdown(
        '<div style="font-family:JetBrains Mono,monospace;font-size:0.58rem;color:#2d4a62;margin-top:-0.5rem">'
        'Each bar = % of observations at that hour that were PEAK (4–5 chargers busy). '
        'Dark bars = not enough data yet. '
        'This drives the 40% predictive component — price rises for busy hours even before traffic arrives.'
        '</div>',
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

    # ── Reservation Slots Table ────────────────────────────────────────────────
    st.markdown('<div class="section-hdr">Reservation Board</div>', unsafe_allow_html=True)

    if _car_res_active:
        schedule = charger_schedule
        all_slots = sorted({s for charger in schedule for s in charger})

        # Charger colors
        charger_colors = ['#00e5ff', '#26de81', '#fed330', '#ff9800', '#7c5cbf']

        tbl = '<div style="overflow-x:auto;margin-bottom:0.5rem">'
        tbl += ('<table style="border-collapse:collapse;width:100%;font-family:JetBrains Mono,monospace;'
                'font-size:0.58rem">')

        # Header row — time slots
        tbl += ('<tr><td style="padding:4px 8px;color:#2d4a62;border-bottom:1px solid #0f1f30;'
                'position:sticky;left:0;background:#060b14;z-index:1;min-width:80px">CHARGER</td>')
        for s in all_slots:
            h, m = (s * 15) // 60, (s * 15) % 60
            tbl += (f'<td style="padding:4px 5px;color:#2d4a62;border-bottom:1px solid #0f1f30;'
                    f'text-align:center;white-space:nowrap">{h:02d}:{m:02d}</td>')
        tbl += '</tr>'

        # One row per charger (1..5)
        for ch in range(STATION_CAPACITY):
            ch_color = charger_colors[ch % len(charger_colors)]
            tbl += (f'<tr><td style="padding:4px 8px;color:{ch_color};border-bottom:1px solid #0f1f30;'
                    f'position:sticky;left:0;background:#060b14;z-index:1;white-space:nowrap">'
                    f'&#9889; CHARGER {ch + 1}</td>')
            for s in all_slots:
                cid = schedule[ch].get(s)
                if cid is not None:
                    short = cid.upper().replace('CAR_', 'C')
                    # Color cell by car SOC
                    cd = st.session_state['cars'].get(cid, {})
                    soc = float(cd.get('current_soc', 0.5))
                    sc = soc_color(soc)
                    tbl += (f'<td style="padding:3px 4px;text-align:center;border-bottom:1px solid #0f1f30;'
                            f'background:{sc}12;color:{sc};white-space:nowrap" '
                            f'title="{cid} — SOC {soc*100:.0f}%">{short}</td>')
                else:
                    tbl += (f'<td style="padding:3px 4px;text-align:center;border-bottom:1px solid #0f1f30;'
                            f'color:#0f1f30">-</td>')
            tbl += '</tr>'

        # Utilization row — how many chargers busy per slot
        tbl += ('<tr><td style="padding:4px 8px;color:#4a6070;border-top:1px solid #1a2d44;'
                'position:sticky;left:0;background:#060b14;z-index:1;font-weight:500">LOAD</td>')
        for s in all_slots:
            count = sum(1 for ch in range(STATION_CAPACITY) if s in schedule[ch])
            pct = int(count / STATION_CAPACITY * 100)
            bar_color = '#26de81' if count <= 2 else '#fed330' if count <= 4 else '#ff4b4b'
            tbl += (f'<td style="padding:3px 4px;text-align:center;border-top:1px solid #1a2d44">'
                    f'<div style="color:{bar_color};font-weight:500;margin-bottom:2px">{count}/{STATION_CAPACITY}</div>'
                    f'<div style="height:3px;background:#0a1525;border-radius:2px;overflow:hidden">'
                    f'<div style="height:100%;width:{pct}%;background:{bar_color};border-radius:2px"></div>'
                    f'</div></td>')
        tbl += '</tr>'

        tbl += '</table></div>'
        st.markdown(tbl, unsafe_allow_html=True)
    else:
        st.markdown(
            '<div style="font-family:JetBrains Mono,monospace;font-size:0.65rem;color:#2d4a62;'
            'padding:0.8rem;background:#080f1a;border:1px solid #0f1f30;border-radius:8px;'
            'text-align:center">No active reservations</div>',
            unsafe_allow_html=True)


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

    _station_html = '<div style="display:flex;gap:8px;flex-wrap:wrap;margin-bottom:1rem">'
    for _slot in range(STATION_CAPACITY):
        if _slot in _charger_to_car:
            _cid   = _charger_to_car[_slot]
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
                          letter-spacing:0.08em;margin-bottom:4px">CHARGER {_slot + 1}</div>
              <div style="font-family:JetBrains Mono,monospace;font-size:0.8rem;color:#2d4a62">FREE</div>
              <div style="height:4px;background:#0a1525;border-radius:2px;margin-top:5px"></div>
            </div>'''
    _station_html += '</div>'
    st.markdown(_station_html, unsafe_allow_html=True)

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
                      # Decision badge
                      f'<div style="flex-shrink:0"><span class="badge {badge_cls}" '
                      f'style="font-size:0.55rem;padding:0.15rem 0.45rem">{badge_html}</span></div>'
                    f'</div>'
                )
            except Exception as e:
                all_cards_html += f'<div style="color:#ff4b4b;font-size:0.8rem;padding:0.5rem">Error [{car_id}]: {e}</div>'

        st.markdown(all_cards_html, unsafe_allow_html=True)

    else:
        st.info("Waiting for car telemetry…")
        st.caption(f"Consumer ready: {st.session_state['kafka_ready']} | Messages: {st.session_state['total_messages']}")

# ── Auto-refresh ───────────────────────────────────────────────────────────────
time.sleep(1)
st.rerun()