#!/bin/bash

# Always run from the project root regardless of where the script is called from
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

# Configuration — use docker-compose exec by service name (portable across Docker versions)
DOCKER_COMPOSE="docker-compose -f docker/docker-compose.yml"

echo "=== 1. Building & Starting Infrastructure (ARM64 Optimized) ==="
$DOCKER_COMPOSE build
$DOCKER_COMPOSE up -d

echo "Waiting for Flink JobManager to be ready..."
until $DOCKER_COMPOSE exec -T flink-jobmanager flink list > /dev/null 2>&1; do
    echo "  JobManager not ready yet, retrying in 3s..."
    sleep 3
done
echo "  JobManager is ready."

echo "=== 2. Resetting Kafka Topics (delete + recreate to clear stale messages) ==="
for TOPIC in energy_data cars_real charging_commands reservation_status; do
    $DOCKER_COMPOSE exec -T kafka kafka-topics --delete --topic $TOPIC --bootstrap-server localhost:9092 2>/dev/null || true
done
sleep 2
for TOPIC in energy_data cars_real charging_commands reservation_status; do
    $DOCKER_COMPOSE exec -T kafka kafka-topics --create --topic $TOPIC --bootstrap-server localhost:9092 --partitions 1 --replication-factor 1
done
echo "  Topics reset."

echo "=== 3. Cancelling any previous Flink jobs ==="
# Multiple deploy.sh runs accumulate jobs that hold TaskManager slots.
# Cancel all of them before submitting a fresh one.
OLD_JOBS=$($DOCKER_COMPOSE exec -T flink-jobmanager flink list 2>/dev/null | grep -oE '[0-9a-f]{32}')
if [ -n "$OLD_JOBS" ]; then
    for JOB_ID in $OLD_JOBS; do
        echo "  Cancelling job $JOB_ID..."
        $DOCKER_COMPOSE exec -T flink-jobmanager flink cancel "$JOB_ID" 2>/dev/null || true
    done
    sleep 3
    echo "  Old jobs cancelled."
else
    echo "  No previous jobs found."
fi

echo "=== 4. Submitting Flink Job ==="
# Note: The project directory is mounted to /opt/flink/project via docker-compose volumes
$DOCKER_COMPOSE exec -T flink-jobmanager flink run -d -pyfs /opt/flink/project/optimizers -py /opt/flink/project/flink_job/process_stream.py

echo "Waiting for Flink job to reach RUNNING state..."
until $DOCKER_COMPOSE exec -T flink-jobmanager flink list 2>/dev/null | grep -q "RUNNING"; do
    echo "  Job not running yet, retrying in 2s..."
    sleep 2
done
echo "  Flink job is RUNNING."

echo "=== 5. Starting Producers ==="
export KAFKA_BOOTSTRAP_SERVERS=localhost:9092

# Kill any leftover producer processes from previous runs
pkill -f produce_energy_data.py 2>/dev/null || true
pkill -f produce_car_data.py    2>/dev/null || true
sleep 1

cd producers
# Start car producer so Flink has input and begins firing timer ticks
nohup python -u produce_car_data.py > cars.log 2>&1 &
echo "  Car Producer started (PID $!)"

# Start energy producer — it listens to Flink's charging commands to count
# active chargers and broadcasts the real-time dynamic price every tick.
nohup python -u produce_energy_data.py > energy.log 2>&1 &
echo "  Energy Producer started (PID $!)"
cd ..
echo "  All producers started. Logs: producers/cars.log, producers/energy.log"


echo "=== 6. Starting Dashboard ==="
export KAFKA_BOOTSTRAP_SERVERS=localhost:9092
# Kill any previous dashboard so Streamlit session state is reset on each deploy
pkill -f "streamlit run dashboard/dashboard.py" 2>/dev/null || true
sleep 2
nohup streamlit run dashboard/dashboard.py > dashboard/dashboard.log 2>&1 &
echo "  Dashboard started (PID $!). Log: dashboard/dashboard.log"

echo ""
echo "=== All systems running ==="
echo "  Dashboard → http://localhost:8501"
echo "  To stop producers: pkill -f produce_"
echo "  To stop dashboard: pkill -f streamlit"
echo "  To stop Docker:    docker-compose -f docker/docker-compose.yml down"
