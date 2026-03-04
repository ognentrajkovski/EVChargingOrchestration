#!/bin/bash

# Kill any existing python producers
pkill -f produce_energy_data.py
pkill -f produce_car_data.py

echo "Starting all producers..."

# Start Energy Producer
nohup python -u produce_energy_data.py > energy.log 2>&1 &
echo "Started Energy Producer (PID $!)"

# Start Car Producer
nohup python -u produce_car_data.py > cars.log 2>&1 &
echo "Started Car Producer (PID $!)"

echo "All producers running in background."
echo "Logs: energy.log, cars.log"
echo "To stop them, run: pkill -f produce_"
