import json
import random
import time
import numpy as np

# Simulation Constants
SIMULATION_INTERVAL_MINS = 15
TOTAL_INTERVALS = 96
STATION_CAPACITY = 5
MAX_POWER_PER_CHARGER = 11.0  # kW
CAR_BATTERY_CAPACITY_KWH = 60.0

# Thresholds
MIN_SOC_EMERGENCY = 0.15

# CHANGED: Allow single 15-min blocks to prevent fragmentation issues
MIN_DURATION_BLOCKS = 1
MAX_SESSIONS_PER_DAY = 3


def smart_heuristic_schedule(prices, cars, current_idx=0, sessions_used=None, station_capacity=None):
    # Input validation
    if not prices:
        return {c['id']: [] for c in cars if c.get('id')}
    prices = [p if (p is not None and not np.isnan(float(p)) and float(p) >= 0) else 999.0 for p in prices]
    cars = [c for c in cars if c.get('id') and c.get('current_soc') is not None and c.get('current_battery_kwh') is not None]
    if not cars:
        return {}

    total_intervals = len(prices)
    # print(f"Running Smart Heuristic for {len(cars)} cars at interval {current_idx}...")
    start_time = time.time()

    # 1. Analyze Prices
    avg_price = np.mean(prices)
    very_low_price_threshold = np.percentile(prices, 10)  # Bottom 10%

    # 2. Sort Cars by Priority (Descending) & Urgency (Low SOC first)
    cars.sort(key=lambda x: (x['priority'], -x['current_soc']), reverse=True)

    # 3. Initialize Schedule & Tracking
    _capacity = station_capacity if station_capacity and station_capacity > 0 else STATION_CAPACITY
    schedule = {c['id']: [] for c in cars}
    station_load = [0] * total_intervals
    _used = sessions_used or {}
    car_sessions = {c['id']: _used.get(c['id'], 0) for c in cars}

    # 4. Allocation Loop
    for car in cars:
        current_kwh = car['current_battery_kwh']
        current_soc = car['current_soc']
        departure = total_intervals

        # Determine Strategy
        target_soc = 0.0
        price_limit = 999999.0
        min_blocks_needed = MIN_DURATION_BLOCKS

        # Strategy A: Emergency (< 15%) — charge at any price
        if current_soc < 0.15:
            target_soc = 0.40
            price_limit = 999999.0
            min_blocks_needed = 1

        # Strategy B: Low (15% - 40%) — charge at average price or below
        elif current_soc < 0.40:
            target_soc = 0.60
            price_limit = avg_price
            min_blocks_needed = 1

        # Strategy C: Medium (40% - 60%) — charge only below average price
        elif current_soc < 0.60:
            target_soc = 0.80
            price_limit = avg_price * 0.9  # Strictly below average
            min_blocks_needed = 1

        # Strategy D: High (60% - 80%) — charge only at bottom 10% cheapest
        elif current_soc < 0.80:
            target_soc = 1.00
            price_limit = very_low_price_threshold
            min_blocks_needed = 1

        # Strategy E: Full (>= 80%) — charge only if price is negative
        else:
            target_soc = 1.00
            price_limit = 0.0  # Only negative prices
            min_blocks_needed = 1

        # 5. Build contiguous windows of available slots
        # Group consecutive available slots into windows, then rank windows
        # by average price so we fill cheapest windows first — this ensures
        # sessions are dense blocks at genuinely cheap periods, not scattered
        # individual cheap slots that each become their own "session".
        available = []
        for t in range(current_idx, min(departure, total_intervals)):
            if station_load[t] < _capacity and prices[t] <= price_limit:
                available.append(t)

        # Emergency: just charge immediately, cheapest window second
        if current_soc < MIN_SOC_EMERGENCY:
            available.sort()  # Time first for emergency
        else:
            available.sort()  # Keep time order; we'll pick windows by avg price

        # Build contiguous windows from available slots
        windows = []
        if available:
            win_start = available[0]
            win_slots = [available[0]]
            for t in available[1:]:
                if t == win_slots[-1] + 1:
                    win_slots.append(t)
                else:
                    windows.append(win_slots)
                    win_slots = [t]
            windows.append(win_slots)

        # Rank windows by average price (cheapest first), emergency by time
        if current_soc >= MIN_SOC_EMERGENCY:
            windows.sort(key=lambda w: np.mean([prices[t] for t in w]))

        final_schedule = []
        sessions_count = 0

        for window in windows:
            if sessions_count >= MAX_SESSIONS_PER_DAY:
                break
            if current_soc >= target_soc:
                break

            session_slots = []
            for t in window:
                if current_soc >= target_soc:
                    break
                if station_load[t] < _capacity:
                    energy_added = MAX_POWER_PER_CHARGER * 0.25
                    current_kwh  = min(CAR_BATTERY_CAPACITY_KWH,
                                       current_kwh + energy_added)
                    current_soc  = current_kwh / CAR_BATTERY_CAPACITY_KWH
                    session_slots.append(t)

            if len(session_slots) >= min_blocks_needed:
                for t in session_slots:
                    final_schedule.append((t, prices[t]))
                    station_load[t] += 1
                sessions_count += 1

        # Sort by time before saving — windows were added in price order,
        # but the plan must be in chronological order for interval matching to work
        final_schedule.sort(key=lambda x: x[0])

        # Save to main schedule
        for t, p in final_schedule:
            schedule[car['id']].append({
                "interval": t,
                "power_kw": MAX_POWER_PER_CHARGER,
                "price": p
            })

    # print(f"Scheduling complete in {time.time() - start_time:.4f} seconds.")
    return schedule