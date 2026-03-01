import streamlit as st
from streamlit.runtime.scriptrunner import add_script_run_ctx
import pandas as pd
import json
import time
import threading
from kafka import KafkaConsumer
import altair as alt
import math

# --- Global State ---
if 'global_data' not in st.session_state:
    st.session_state.global_data = {
        'prices': [],
        'cars': {},
        'commands': []
    }

TOPICS = ['energy_data', 'cars_real', 'charging_commands']
BOOTSTRAP_SERVERS = 'localhost:9092'

def consume_messages(data_dict):
    consumer = KafkaConsumer(
        *TOPICS,
        bootstrap_servers=BOOTSTRAP_SERVERS,
        value_deserializer=lambda x: json.loads(x.decode('utf-8')),
        auto_offset_reset='latest'
    )
    for message in consumer:
        topic = message.topic
        data = message.value
        if topic == 'energy_data':
            # Store prices. We expect 96 at a time for Tomorrow.
            new_price = {
                'price': float(data.get('AT_price_day_ahead', 0)),
                'timestamp': data.get('sent_at', time.time())
            }
            data_dict['prices'].append(new_price)
            if len(data_dict['prices']) > 192: data_dict['prices'].pop(0)
        elif topic == 'cars_real':
            data_dict['cars'][data.get('id')] = data
        elif topic == 'charging_commands':
            data_dict['commands'].append(data)
            if len(data_dict['commands']) > 50: data_dict['commands'].pop(0)

if 'consumer_thread' not in st.session_state:
    t = threading.Thread(target=consume_messages, args=(st.session_state.global_data,), daemon=True)
    add_script_run_ctx(t)
    t.start()
    st.session_state.consumer_thread = t

# --- Time Calculation (1h = 5s) ---
commands = st.session_state.global_data.get('commands', [])
sim_hours = 0
sim_minutes = 0

if commands:
    latest_cmd = commands[-1]
    latest_idx = latest_cmd.get('interval_idx', 0)
    cmd_recv_time = latest_cmd.get('timestamp', time.time())
    elapsed_since_cmd = time.time() - cmd_recv_time
    # Interpolate for smooth clock
    exact_interval = latest_idx + (elapsed_since_cmd / 1.25)
    total_sim_minutes = int(exact_interval * 15)
    sim_hours = (total_sim_minutes // 60) % 24
    sim_minutes = total_sim_minutes % 60

digital_time = f"{sim_hours:02d}:{sim_minutes:02d}"

# Analog Clock SVG
hour_angle = (sim_hours % 12 + sim_minutes / 60.0) * 30
minute_angle = sim_minutes * 6
hx, hy = 50 + 25 * math.sin(math.radians(hour_angle)), 50 - 25 * math.cos(math.radians(hour_angle))
mx, my = 50 + 35 * math.sin(math.radians(minute_angle)), 50 - 35 * math.cos(math.radians(minute_angle))
numbers_svg = "".join([f'<text x="{50 + 35 * math.sin(math.radians(i*30))}" y="{50 - 35 * math.cos(math.radians(i*30))+3}" font-size="10" text-anchor="middle" font-family="sans-serif" font-weight="bold" fill="#555">{i}</text>' for i in range(1, 13)])

svg_clock = f"""<div style="display: flex; align-items: center; gap: 20px;">
    <svg width="100" height="100"><circle cx="50" cy="50" r="45" stroke="gray" stroke-width="2" fill="white" />{numbers_svg}
    <line x1="50" y1="50" x2="{hx}" y2="{hy}" stroke="black" stroke-width="4" stroke-linecap="round"/><line x1="50" y1="50" x2="{mx}" y2="{my}" stroke="blue" stroke-width="2" stroke-linecap="round"/><circle cx="50" cy="50" r="3" fill="black" /></svg>
    <div style="font-family: monospace; font-size: 3rem; font-weight: bold; color: #333;">{digital_time}</div>
</div>"""

# --- UI Layout ---
st.set_page_config(layout="wide", page_title="EV Fleet Optimizer")
st.title("🚗 Real-Time Day-Ahead EV Optimizer")
st.html(svg_clock)
st.markdown("---")

# Metrics
col1, col2, col3, col4 = st.columns(4)
cars = st.session_state.global_data['cars']
avg_soc = sum(c.get('current_soc', 0) for c in cars.values()) / len(cars) if cars else 0
with col1: st.metric("Fleet Size", len(cars))
with col2: st.metric("Charging Now", sum(1 for c in cars.values() if c.get('plugged_in')))
with col3: st.metric("Avg Battery", f"{avg_soc*100:.1f}%")
with col4: 
    prices = st.session_state.global_data['prices']
    latest_p = prices[-1]['price'] if prices else 0
    st.metric("Latest Market Price", f"€{latest_p:.2f}")

# Charts
c1, c2 = st.columns([2, 1])
with c1:
    st.subheader("📈 Day-Ahead Energy Prices (24h Format)")
    if prices:
        # Create a 24h timeline for the chart
        chart_data = []
        for i, p in enumerate(prices[-96:]): # Show the most recent 24h block
            chart_data.append({"Time": f"{(i*15)//60:02d}:{(i*15)%60:02d}", "Price": p['price']})
        df_p = pd.DataFrame(chart_data)
        st.altair_chart(alt.Chart(df_p).mark_line(color='orange', strokeWidth=3).encode(x=alt.X('Time:O', title="Hour of Day", axis=alt.Axis(labelAngle=-45)), y=alt.Y('Price:Q', title="Price (€/MWh)")), use_container_width=True)
    else: st.info("Waiting for 13:00 Market Clearing...")

with c2:
    st.subheader("🔋 Fleet Battery Status")
    if cars:
        df_c = pd.DataFrame(list(cars.values()))
        st.altair_chart(alt.Chart(df_c).mark_bar().encode(x='id:N', y=alt.Y('current_soc:Q', scale=alt.Scale(domain=[0, 1])), color=alt.Color('plugged_in:N', scale=alt.Scale(range=['#ff4b4b', '#29b09d']))), use_container_width=True)

# Schedules
st.subheader("📅 Charging Predictions (Next 24h)")
if commands:
    latest_plans = {cmd.get('car_id'): cmd.get('plan', []) for cmd in commands}
    sc1, sc2 = st.columns(2)
    
    with sc1:
        st.markdown("#### 📍 Today's Active Plan")
        has_today = False
        for cid, plan in latest_plans.items():
            p = [s for s in plan if s['interval'] < 96]
            if p:
                has_today = True
                sh, sm, eh, em = (p[0]['interval']*15)//60, (p[0]['interval']*15)%60, ((p[-1]['interval']+1)*15)//60, ((p[-1]['interval']+1)*15)%60
                if eh >= 24: eh = 0
                st.info(f"**{cid.upper()}** | 🟢 `{sh:02d}:{sm:02d}` → 🔴 `{eh:02d}:{em:02d}`")
        if not has_today: st.write("No charging required today.")

    with sc2:
        st.markdown("#### ⏩ Tomorrow's Day-Ahead Prediction")
        has_tmrw = False
        for cid, plan in latest_plans.items():
            p = [s for s in plan if s['interval'] >= 96]
            if p:
                has_tmrw = True
                sh, sm = ((p[0]['interval']-96)*15)//60, ((p[0]['interval']-96)*15)%60
                eh, em = ((p[-1]['interval']-95)*15)//60, ((p[-1]['interval']-95)*15)%60
                if eh >= 24: eh = 0
                st.success(f"**{cid.upper()}** | 🟢 `{sh:02d}:{sm:02d}` → 🔴 `{eh:02d}:{em:02d}`")
        if not has_tmrw: st.write("Waiting for 13:00 price release...")
else: st.write("Waiting for Flink initialization...")

time.sleep(1)
st.rerun()