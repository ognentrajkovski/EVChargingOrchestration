#!/bin/bash
export MSYS_NO_PATHCONV=1
export MSYS2_ARG_CONV_EXCL="*"

# Always run from the project root regardless of where the script is called from
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

# Support both old docker-compose and new docker compose plugin
if command -v docker-compose &> /dev/null; then
    DOCKER_COMPOSE="docker-compose -f docker/docker-compose.yml"
else
    DOCKER_COMPOSE="docker compose -f docker/docker-compose.yml"
fi

echo "=== 1. Building & Starting Infrastructure (AMD64 Optimized) ==="
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
    $DOCKER_COMPOSE exec -T kafka kafka-topics --delete --topic $TOPIC --bootstrap-server kafka:9092 2>/dev/null || true
done
sleep 2
for TOPIC in energy_data cars_real charging_commands reservation_status; do
    $DOCKER_COMPOSE exec -T kafka kafka-topics --create --topic $TOPIC --bootstrap-server kafka:9092 --partitions 1 --replication-factor 1
done
echo "  Topics reset."

echo "=== 3. Cancelling any previous Flink jobs ==="
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
# Flink 1.17 requires -pyfs to be a zip file, not a raw directory
$DOCKER_COMPOSE exec -T flink-jobmanager \
    bash -c "cd /opt/flink/project && zip -r /opt/flink/project/optimizers.zip optimizers/"

$DOCKER_COMPOSE exec -T flink-jobmanager flink run -d \
    -pyfs /opt/flink/project/optimizers.zip \
    -py /opt/flink/project/flink_job/process_stream.py

echo "Waiting for Flink job to reach RUNNING state..."
until $DOCKER_COMPOSE exec -T flink-jobmanager flink list 2>/dev/null | grep -q "RUNNING"; do
    echo "  Job not running yet, retrying in 2s..."
    sleep 2
done
echo "  Flink job is RUNNING."

echo "=== 5. Starting Producers ==="
export KAFKA_BOOTSTRAP_SERVERS=localhost:9092

taskkill //F //FI "IMAGENAME eq python.exe" 2>/dev/null || true
sleep 1

cd producers
cmd.exe /c "start /b python -u produce_car_data.py > cars.log 2>&1"
echo "  Car Producer started"
cmd.exe /c "start /b python -u produce_energy_data.py > energy.log 2>&1"
echo "  Energy Producer started"
cd ..

echo "=== 6. Starting Dashboard ==="
export KAFKA_BOOTSTRAP_SERVERS=localhost:9092
taskkill //F //FI "WINDOWTITLE eq streamlit*" 2>/dev/null || true
sleep 1

cmd.exe /c "start /b streamlit run dashboard/dashboard.py > dashboard/dashboard.log 2>&1"
echo "  Dashboard started"

echo ""
echo "=== All systems running ==="
echo "  Dashboard → http://localhost:8501"
echo "  Flink UI  → http://localhost:8081"
echo "  To stop Docker: $DOCKER_COMPOSE down"