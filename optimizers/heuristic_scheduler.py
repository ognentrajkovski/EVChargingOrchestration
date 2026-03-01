import json
import random
import time
import numpy as np

# Simulation Constants
SIMULATION_INTERVAL_MINS = 15
TOTAL_INTERVALS = 96
STATION_CAPACITY = 5 
MAX_POWER_PER_CHARGER = 11.0 # kW
CAR_BATTERY_CAPACITY_KWH = 60.0

# Thresholds
MIN_SOC_EMERGENCY = 0.15 # 15% - Must charge immediately to this level
TARGET_SOC_NORMAL = 0.70 # Charge to 70% if price < avg
TARGET_SOC_TOPUP = 0.80 # Charge to 80% if price < very low

MIN_DURATION_BLOCKS = 2 # 30 mins (2 * 15) unless > 80% SOC
MAX_SESSIONS_PER_DAY = 3

def smart_heuristic_schedule(prices, cars, current_idx=0):
    total_intervals = len(prices)
    print(f"Running Smart Heuristic for {len(cars)} cars at interval {current_idx}...")
    start_time = time.time()
    
    # 1. Analyze Prices
    avg_price = np.mean(prices)
    very_low_price_threshold = np.percentile(prices, 10) # Bottom 10%

    print(f"Price Stats: Avg={avg_price:.2f}, Low (<10%)={very_low_price_threshold:.2f}")

    # 2. Sort Cars by Priority (Descending) & Urgency (Low SOC first)
    # Sort key: (Priority, -CurrentSOC) -> High priority first, then lowest battery
    cars.sort(key=lambda x: (x['priority'], -x['current_soc']), reverse=True)

    # 3. Initialize Schedule & Tracking
    schedule = {c['id']: [] for c in cars}
    station_load = [0] * total_intervals # Count active chargers per interval
    car_sessions = {c['id']: 0 for c in cars} # Count sessions per car

    # 4. Allocation Loop
    for car in cars:
        current_kwh = car['current_battery_kwh']
        current_soc = car['current_soc']
        departure = car.get('departure_time_idx', total_intervals)
        
        # Determine Strategy
        target_soc = 0.0
        price_limit = 999999.0
        min_blocks_needed = MIN_DURATION_BLOCKS # Default 30 mins
        
        # Strategy A: Emergency (< 30%)
        if current_soc < MIN_SOC_EMERGENCY:
            target_soc = MIN_SOC_EMERGENCY
            price_limit = 999999.0 # Price doesn't matter, must charge!
            min_blocks_needed = 1 # Must charge immediately, don't drop small blocks
            print(f"  [URGENT] {car['id']} (SOC {current_soc:.2f}) needs emergency charge to {target_soc}")

        # Strategy B: Normal (30% - 70%)
        elif current_soc < TARGET_SOC_NORMAL:
            target_soc = TARGET_SOC_NORMAL
            price_limit = avg_price # Only charge if cheaper than average
            min_blocks_needed = 1 # Allow 15 min blocks
            print(f"  [NORMAL] {car['id']} (SOC {current_soc:.2f}) aims for {target_soc} (Price Limit: {price_limit:.2f})")

        # Strategy C: Top-Up (> 80%)
        elif current_soc >= 0.80:
            target_soc = 1.00 # Max it out if cheap
            price_limit = very_low_price_threshold # Only charge if dirt cheap
            min_blocks_needed = 1 # Allow 15 min top-ups
            print(f"  [TOP-UP] {car['id']} (SOC {current_soc:.2f}) considers top-up (Limit: {price_limit:.2f})")
        
        # Strategy D: Leave for Next Day (70% - 80%)
        else:
             # Already > 70%, just maintain unless very cheap
             target_soc = TARGET_SOC_TOPUP
             price_limit = very_low_price_threshold
             min_blocks_needed = 1

        # 5. Find Valid Intervals (Respecting Constraints)
        # Scan from NOW until DEPARTURE
        
        valid_slots = []
        for t in range(current_idx, total_intervals):
            if t >= departure: break # Car leaves
            if station_load[t] < STATION_CAPACITY:
                valid_slots.append((prices[t], t))
        
        # Sort slots depending on strategy
        if current_soc < MIN_SOC_EMERGENCY:
            # URGENT: Prioritize TIME (charge NOW)
            valid_slots.sort(key=lambda x: x[1])
        else:
            # NORMAL/TOP-UP: Prioritize PRICE (Cheapest first)
            valid_slots.sort(key=lambda x: x[0])
        
        intervals_selected = []
        
        # Greedy Selection
        for price, t in valid_slots:
            # Overrule limits if price is free or negative!
            if price > price_limit and price > 0:
                 continue
            
            # Check if we reached target
            if current_soc >= target_soc and price > 0:
                break
                
            # Check Session Limit (Rough Check: Are we starting a NEW session?)
            # Actually, greedy approach makes this hard. We just pick best slots, then GROUP them.
            
            # Charge
            energy_added = MAX_POWER_PER_CHARGER * 0.25 # 15 mins
            
            current_kwh += energy_added
            current_soc = current_kwh / CAR_BATTERY_CAPACITY_KWH
            
            intervals_selected.append((t, price))
            
        # Post-Processing: Enforce Min Duration & Max Sessions
        intervals_selected.sort(key=lambda x: x[0]) # Sort by time
        
        final_schedule = []
        if not intervals_selected:
            continue
            
        current_block = []
        sessions_count = 0
        
        for i, (t, p) in enumerate(intervals_selected):
            # Check continuity
            if not current_block:
                current_block.append((t, p))
            elif t == current_block[-1][0] + 1:
                current_block.append((t, p))
            else:
                # Block ended. Check constraints.
                if len(current_block) >= min_blocks_needed:
                    if sessions_count < MAX_SESSIONS_PER_DAY:
                        # Commit Block
                        for slot_t, slot_p in current_block:
                            # Verify station capacity again (in case other cars took it? No, we process sequentially)
                            if station_load[slot_t] < STATION_CAPACITY:
                                final_schedule.append((slot_t, slot_p))
                                station_load[slot_t] += 1
                            else:
                                # Conflict! Skip this slot.
                                pass 
                        sessions_count += 1
                current_block = [(t, p)]
        
        # Check last block
        if current_block:
             if len(current_block) >= min_blocks_needed:
                if sessions_count < MAX_SESSIONS_PER_DAY:
                    for slot_t, slot_p in current_block:
                        if station_load[slot_t] < STATION_CAPACITY:
                            final_schedule.append((slot_t, slot_p))
                            station_load[slot_t] += 1

        # Save to main schedule
        for t, p in final_schedule:
             schedule[car['id']].append({
                "interval": t,
                "power_kw": MAX_POWER_PER_CHARGER,
                "price": p
            })

    print(f"Scheduling complete in {time.time() - start_time:.4f} seconds.")
    return schedule
