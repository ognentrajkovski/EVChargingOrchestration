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
SIM_MINUTES_PER_REAL_SEC = 12   # 1 real sec = 6 sim min → full day = 240 real sec

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
    'kafka_ready': False,
    'total_messages': 0,
    'poll_count': 0,
    'last_error': None,
    'topic_counts': {'energy_data': 0, 'cars_real': 0, 'charging_commands': 0},
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

                elif topic == TOPIC_CARS:
                    car_id = data['id']
                    st.session_state['cars'][car_id] = data
                    st.session_state['topic_counts']['cars_real'] += 1
                    _dash_log(f"CAR | {car_id} | soc={data.get('current_soc',0)*100:.1f}% | plugged={data.get('plugged_in')}")

                elif topic == TOPIC_COMMANDS:
                    st.session_state['topic_counts']['charging_commands'] += 1
                    car_id = data.get('car_id')
                    plan   = data.get('plan', [])
                    if car_id and plan:
                        intervals = [s['interval'] for s in plan]
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

# ── Advance sim clock (wall-clock anchored — no drift) ────────────────────────
def advance_clock():
    elapsed_real_secs = time.time() - st.session_state['sim_start_wall']
    total_sim_mins    = st.session_state['sim_start_mins'] + elapsed_real_secs * SIM_MINUTES_PER_REAL_SEC
    days_elapsed      = int(total_sim_mins // 1440)
    st.session_state['sim_time_minutes'] = int(total_sim_mins % 1440)
    # Track day rollovers by comparing against stored day count
    expected_day = 1 + days_elapsed
    if expected_day > st.session_state['day_count']:
        st.session_state['day_count'] = expected_day
        st.session_state['today_prices_96']    = st.session_state['tomorrow_prices_96'][:]
        st.session_state['tomorrow_prices_96'] = []

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
hours    = st.session_state['sim_time_minutes'] // 60
mins     = st.session_state['sim_time_minutes'] % 60
time_str = f"{hours:02d}:{mins:02d}"
cur_idx  = st.session_state['sim_time_minutes'] // 15   # current 15-min interval

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
last_price     = prices[-1] if prices else (ref_prices[-1] if ref_prices else None)
price_col      = '#ff4b4b' if has_prices and last_price > p70 else '#26de81' if has_prices and last_price <= p30 else '#fed330'
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
        m1, m2, m3, m4 = st.columns(4)
        m1.metric("p10 — Very Cheap", f"{p10:.1f} €")
        m2.metric("p30 — Cheap",      f"{p30:.1f} €")
        m3.metric("p70 — Normal",     f"{p70:.1f} €")
        last_price = prices[-1] if prices else (ref_prices[-1] if ref_prices else None)
        m4.metric("Current", f"{last_price:.1f} €" if last_price is not None else "—")
    else:
        st.info("⏳ Waiting for energy price data…")

    # ── 96-slot day heatmap ──
    st.markdown('<div class="section-hdr">Today\'s Price Map — 96 × 15-min Intervals</div>', unsafe_allow_html=True)

    # Fallback: if TODAY buffer is empty (old producer only sent TOMORROW), use rolling prices
    effective_today = today_p96 if today_p96 else prices[:96]
    for map_label, map_prices in [("TODAY", effective_today), ("TOMORROW", tomorrow_p96)]:
        if not map_prices:
            st.caption(f"{map_label}: waiting for data…")
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
    st.caption("Each block = 15 min. Outlined block = current interval. Hover for time & price.")

    # ── Per-car charging schedule ──
    st.markdown('<div class="section-hdr">Planned Charging Schedules (Flink Output)</div>', unsafe_allow_html=True)

    plans = st.session_state['charging_plans']
    if plans:
        for day_label, price_buf, offset in [("TODAY", today_p96, 0), ("TOMORROW", tomorrow_p96, 96)]:
            st.markdown(f'<div style="font-family:Share Tech Mono,monospace;font-size:0.65rem;color:#4a6070;margin:4px 0 2px">{day_label}</div>', unsafe_allow_html=True)
            for car_id, planned_slots in sorted(plans.items()):
                planned_set = set(planned_slots)
                row_html = f'<div style="margin-bottom:3px"><span style="font-family:Share Tech Mono,monospace;font-size:0.68rem;color:#7b9bb5;display:inline-block;width:60px">{car_id}</span>'
                row_html += '<div class="sched-row" style="display:inline-flex">'
                for t in range(96):
                    abs_t = t + offset  # absolute interval index (0-95 today, 96-191 tomorrow)
                    if abs_t in planned_set:
                        bg = '#00e5ff'
                    elif t < len(price_buf) and price_buf[t] <= p30:
                        bg = '#26de8130'
                    else:
                        bg = '#1e2d3d'
                    active = ' outline: 1px solid #fff3;' if (offset == 0 and t == cur_idx) else ''
                    row_html += f'<div class="sched-slot" style="background:{bg};{active}"></div>'
                row_html += '</div></div>'
                st.markdown(row_html, unsafe_allow_html=True)
        st.markdown("""
        <div class="legend-row" style="margin-top:0.4rem">
          <span><span class="legend-dot" style="background:#00e5ff"></span>Planned charge</span>
          <span><span class="legend-dot" style="background:#26de81;opacity:0.4"></span>Cheap window (not scheduled)</span>
          <span><span class="legend-dot" style="background:#1e2d3d"></span>No action</span>
        </div>
        """, unsafe_allow_html=True)
    else:
        st.caption("Charging plans appear once Flink starts emitting commands…")


# ═══════════════════════════════════════════════
#  RIGHT — Car Battery Cards
# ═══════════════════════════════════════════════
with right_col:
    st.markdown('<div class="section-hdr">Fleet Status</div>', unsafe_allow_html=True)

    cars = st.session_state['cars']
    if cars:
        # Build ALL cards as one HTML string — Streamlit's parser handles
        # a single large block much more reliably than many small ones.
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
                card_cls    = 'charging' if plugged else ('critical' if is_critical else '')
                batt_cls    = 'charging' if plugged else ''

                if plugged:
                    badge_html = 'Charging'
                    badge_cls  = 'badge-charging'
                    badge_icon = '&#9889;'
                elif is_critical:
                    badge_html = 'Critical'
                    badge_cls  = 'badge-critical'
                    badge_icon = '&#9888;'
                else:
                    badge_html = 'Idle'
                    badge_cls  = 'badge-idle'
                    badge_icon = '&#9679;'

                rate_html = '<span style="font-size:0.7rem;color:#00e5ff;font-family:Share Tech Mono,monospace"> +11.0 kW</span>' if plugged else ''

                plan_set = set(st.session_state['charging_plans'].get(car_id, []))
                slots_html = ""
                for t in range(cur_idx, min(cur_idx + 32, 96)):
                    if t in plan_set:
                        bg = '#00e5ff'
                    elif t < len(today_p96) and today_p96[t] <= p30:
                        bg = '#26de8140'
                    else:
                        bg = '#1e2d3d'
                    slots_html += f'<div class="sched-slot" style="background:{bg};width:8px;display:inline-block"></div>'

                label = car_id.upper().replace("_", " ")

                all_cards_html += (
                    f'<div class="car-card {card_cls}" style="margin-bottom:0.7rem">'
                        f'<div style="display:flex;justify-content:space-between">'
                            f'<div>'
                                f'<div class="car-id">{label}</div>'
                                f'<div class="car-soc-num" style="color:{fill_color}">{soc_pct}'
                                    f'<span style="font-size:1rem;color:#4a6070"> %</span>'
                                    f'{rate_html}'
                                f'</div>'
                                f'<div class="car-meta">{kwh:.1f} kWh &nbsp;·&nbsp; Priority {priority}</div>'
                            f'</div>'
                            f'<div><span class="badge {badge_cls}">{badge_icon} {badge_html}</span></div>'
                        f'</div>'
                        f'<div class="batt-track">'
                            f'<div class="batt-fill {batt_cls}" style="width:{soc_pct}%;background:{fill_color}"></div>'
                        f'</div>'
                        f'<div style="font-size:0.65rem;color:#4a6070;font-family:Share Tech Mono,monospace;margin:0.3rem 0 0.2rem">NEXT 8h PLAN</div>'
                        f'<div style="display:flex;flex-wrap:wrap;gap:2px">{slots_html}</div>'
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