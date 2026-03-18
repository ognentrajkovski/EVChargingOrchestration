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
PRICE_COEFFICIENT    = 1.5
PRICE_HISTORY_LEN    = 120   # ~2 min of ticks at 1 s/tick

# ── Session state ──────────────────────────────────────────────────────────────
defaults = {
    'dynamic_prices':          [],      # rolling list of current_price per tick
    'time_labels':             [],      # rolling list of formatted HH:MM strings corresponding to ticks
    'active_chargers_hist':    [],      # parallel list of active_charger counts
    'cars':                    {},      # car_id → latest telemetry dict
    'car_decisions':           {},      # car_id → 'CHARGING'|'IDLE'|'DEFERRED_PRICE'|'DEFERRED_FULL'|'EMERGENCY'
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
    <div class="clock-label">Price Coeff</div>
    <div class="clock-val">×{1 + PRICE_COEFFICIENT:.1f}</div>
  </div>
</div>
""", unsafe_allow_html=True)

# ── Main columns ───────────────────────────────────────────────────────────────
left_col, right_col = st.columns([3, 2], gap="medium")

# ═══════════════════════════════════════════════
#  LEFT — Price Sparkline + Price Ladder
# ═══════════════════════════════════════════════
with left_col:

    # ── Dynamic price sparkline ──────────────────────────────────────────────
    st.markdown('<div class="section-hdr">Real-Time Price Feed (€/MWh)</div>', unsafe_allow_html=True)

    if dynamic_prices:
        n_pts  = len(dynamic_prices)
        x_vals = list(range(-n_pts + 1, 1))   # 0 = now, negative = past

        # Color each segment by price level
        colors = []
        for p in dynamic_prices:
            if p <= BASE_PRICE * 1.1:
                colors.append('#26de81')
            elif p <= BASE_PRICE * 1.6:
                colors.append('#fed330')
            else:
                colors.append('#ff4b4b')

        fig = go.Figure()
        fig.add_trace(go.Scatter(
            x=x_vals, y=list(dynamic_prices),
            mode='lines',
            line=dict(color='#00e5ff', width=2),
            fill='tozeroy', fillcolor='rgba(0,229,255,0.04)',
            customdata=st.session_state['time_labels'],
            hovertemplate='Time: %{customdata} <br>Price: %{y:.1f} €/MWh<extra></extra>',
            name='Price'
        ))
        # Horizontal reference lines for each charger threshold
        for n in range(STATION_CAPACITY + 1):
            ref_p = BASE_PRICE * (1 + PRICE_COEFFICIENT * n / STATION_CAPACITY)
            dash  = 'solid' if n == current_chargers else 'dot'
            width = 1.5 if n == current_chargers else 0.5
            col   = occupancy_color(n)
            fig.add_hline(
                y=ref_p, line_dash=dash, line_color=col, line_width=width,
                annotation_text=f'{n} charger{"s" if n != 1 else ""}',
                annotation_font_color=col,
                annotation_font_size=9,
                annotation_position='right'
            )
        fig.update_layout(
            height=220,
            paper_bgcolor='#0d1520', plot_bgcolor='#0d1520',
            margin=dict(l=50, r=90, t=10, b=35),
            xaxis=dict(
                title=None,
                tickmode='array',
                tickvals=x_vals[::20],
                ticktext=st.session_state['time_labels'][::20],
                tickfont=dict(color='#4a6070', size=9),
                title_font=dict(color='#4a6070', size=9),
                gridcolor='#1e2d3d', linecolor='#1e2d3d',
                zeroline=False
            ),
            yaxis=dict(
                title='€/MWh',
                tickfont=dict(color='#4a6070', size=9),
                title_font=dict(color='#4a6070', size=9),
                gridcolor='#1e2d3d', linecolor='#1e2d3d',
                zeroline=False
            ),
            showlegend=False,
        )
        st.plotly_chart(fig, use_container_width=True, config={'displayModeBar': False})
    else:
        st.info("⏳ Waiting for dynamic price data…")

    # ── Active charger count history ──────────────────────────────────────────
    if active_chargers_hist and len(active_chargers_hist) > 1:
        n_pts  = len(active_chargers_hist)
        x_vals = list(range(-n_pts + 1, 1))
        fig2   = go.Figure()
        fig2.add_trace(go.Scatter(
            x=x_vals, y=list(active_chargers_hist),
            mode='lines',
            line=dict(color='#7c5cbf', width=1.5),
            fill='tozeroy', fillcolor='rgba(124,92,191,0.06)',
            customdata=st.session_state['time_labels'],
            hovertemplate='Time: %{customdata} <br>%{y} chargers active<extra></extra>',
        ))
        fig2.update_layout(
            height=100,
            paper_bgcolor='#0d1520', plot_bgcolor='#0d1520',
            margin=dict(l=50, r=90, t=5, b=30),
            xaxis=dict(
                tickmode='array', tickvals=x_vals[::20], ticktext=st.session_state['time_labels'][::20],
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

    ladder_html = '<div style="display:flex;flex-direction:column;gap:4px">'
    for n in range(STATION_CAPACITY + 1):
        ref_p     = BASE_PRICE * (1 + PRICE_COEFFICIENT * n / STATION_CAPACITY)
        is_active = (n == current_chargers)
        col       = occupancy_color(n)
        fill_pct  = int(ref_p / (BASE_PRICE * (1 + PRICE_COEFFICIENT)) * 100)
        outline   = f'border:1px solid {col}60;' if is_active else 'border:1px solid #0f1f30;'
        bg_active = f'background:#0d1520;{outline}' if is_active else 'background:#080f1a;border:1px solid #0f1f30;'
        label     = f'{"▶ " if is_active else ""}{n}/{STATION_CAPACITY} charger{"s" if n != 1 else ""} busy'
        shadow    = f'box-shadow:0 0 12px {col}30;' if is_active else ''

        ladder_html += f'''
        <div style="{bg_active}{shadow}border-radius:8px;padding:0.45rem 0.8rem;display:flex;align-items:center;gap:12px;transition:all 0.3s ease">
          <div style="font-family:JetBrains Mono,monospace;font-size:0.65rem;color:{"#e2eaf4" if is_active else "#4a6070"};width:160px;flex-shrink:0">{label}</div>
          <div style="flex:1;height:6px;background:#0a1525;border-radius:3px;overflow:hidden">
            <div style="width:{fill_pct}%;height:100%;background:{col};border-radius:3px;{"animation:batt-shimmer 2s linear infinite;background-size:200% auto;" if is_active else ""}"></div>
          </div>
          <div style="font-family:JetBrains Mono,monospace;font-size:0.75rem;color:{col};width:80px;text-align:right;flex-shrink:0">{ref_p:.1f} €/MWh</div>
        </div>'''
    ladder_html += '</div>'
    st.markdown(ladder_html, unsafe_allow_html=True)

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
        ('<15%',   '< 15%',    'Emergency — charges at any price', '#ff4b4b', 1.0),
        ('15-40%', '15 – 40%', f'Urgent — pays up to {BASE_PRICE*3.0:.0f} €/MWh', '#fed330', 3.0),
        ('40-60%', '40 – 60%', f'Moderate — pays up to {BASE_PRICE*2.2:.0f} €/MWh', '#26de81', 2.2),
        ('60-80%', '60 – 80%', f'Comfortable — pays up to {BASE_PRICE*1.8:.0f} €/MWh', '#00e5ff', 1.8),
        ('>80%',   '≥ 80%',    f'Opportunistic — pays up to {BASE_PRICE*1.4:.0f} €/MWh', '#7c5cbf', 1.4),
    ]

    soc_html = '<div style="display:flex;flex-direction:column;gap:4px">'
    for band_key, soc_label, desc, col, mult in soc_bands:
        fill_pct = int(mult / (1 + PRICE_COEFFICIENT) * 100)
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


# ═══════════════════════════════════════════════
#  RIGHT — Charging Stations + Fleet Status
# ═══════════════════════════════════════════════
with right_col:

    # ── Charging station slots ────────────────────────────────────────────────
    st.markdown('<div class="section-hdr">Charging Stations</div>', unsafe_allow_html=True)

    cars = st.session_state['cars']
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
        real_cars_map = {k: v for k, v in sorted(cars.items()) if not k.endswith('_test')}
        test_cars_map = {k: v for k, v in sorted(cars.items()) if k.endswith('_test')}

        for group_cars, group_label, group_color in [
            (real_cars_map, '⚡ Smart Bidder', '#00e5ff'),
            (test_cars_map,  '🔌 Naive (always charges)', '#ff5252'),
        ]:
            if not group_cars:
                continue
            st.markdown(
                f'<div style="font-family:JetBrains Mono,monospace;font-size:0.65rem;'
                f'color:{group_color};margin:8px 0 4px;border-bottom:1px solid {group_color}33;'
                f'padding-bottom:3px">{group_label}</div>',
                unsafe_allow_html=True)

            all_cards_html = ""
            for car_id, car in group_cars.items():
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