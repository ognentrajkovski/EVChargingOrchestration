"""
EV Fleet Shared Logger
Import this in any file: from ev_logger import log, log_schedule, log_charging, log_price

Writes to: /tmp/ev_fleet.log
"""
import logging
import json
import os
from datetime import datetime

LOG_FILE = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'ev_fleet.log')  # always at project root

# One shared file handler
_handler = logging.FileHandler(LOG_FILE, mode='a', encoding='utf-8')
_handler.setFormatter(logging.Formatter('%(asctime)s [%(name)s] %(message)s', datefmt='%H:%M:%S'))

def _get(name):
    log = logging.getLogger(name)
    if not log.handlers:
        log.addHandler(_handler)
        log.setLevel(logging.DEBUG)
    return log

# ── Convenience functions ──────────────────────────────────────────────────────

def log(component, msg):
    _get(component).info(msg)

def log_price(msg_type, price, total_today, total_tomorrow):
    _get('PRICE').info(f"{msg_type} | price={price:.2f} | today_buf={total_today} | tmrw_buf={total_tomorrow}")

def log_car(car_id, soc, plugged, source='sensor'):
    _get('CAR').info(f"{car_id} | soc={soc*100:.1f}% | plugged={plugged} | src={source}")

def log_schedule(car_id, intervals, current_idx, reason='replan'):
    first8 = intervals[:8]
    _get('SCHED').info(f"{car_id} | reason={reason} | idx={current_idx} | slots={first8}... ({len(intervals)} total)")

def log_command(car_id, action, new_soc, idx):
    soc_str = f"{new_soc*100:.1f}%" if new_soc else "None"
    _get('CMD').info(f"{car_id} | {action} | new_soc={soc_str} | idx={idx}")

def log_charging_decision(car_id, idx, plan_intervals, should_charge):
    match = idx in set(plan_intervals)
    _get('CHARGE').info(f"{car_id} | idx={idx} | in_plan={match} | should_charge={should_charge} | plan_has={len(plan_intervals)} slots")

def log_error(component, error):
    _get(component).error(f"ERROR: {error}")

# Write a separator on import so log sections are easy to find
_get('SYSTEM').info("=" * 60)
_get('SYSTEM').info(f"EV Fleet Logger started at {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
_get('SYSTEM').info("=" * 60)