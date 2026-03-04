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

echo "=== 2. Creating Kafka Topics ==="
$DOCKER_COMPOSE exec -T kafka kafka-topics --create --topic energy_data --bootstrap-server localhost:9092 --partitions 1 --replication-factor 1 --if-not-exists
$DOCKER_COMPOSE exec -T kafka kafka-topics --create --topic cars_real --bootstrap-server localhost:9092 --partitions 1 --replication-factor 1 --if-not-exists
$DOCKER_COMPOSE exec -T kafka kafka-topics --create --topic charging_commands --bootstrap-server localhost:9092 --partitions 1 --replication-factor 1 --if-not-exists

echo "=== 3. Submitting Flink Job ==="
# Note: The project directory is mounted to /opt/flink/project via docker-compose volumes
$DOCKER_COMPOSE exec -T flink-jobmanager flink run -d -pyfs /opt/flink/project/optimizers -py /opt/flink/project/flink_job/process_stream.py
echo "=== 4. Starting Producers ==="
cd producers
export KAFKA_BOOTSTRAP_SERVERS=localhost:9092
bash run_all_producers.sh
cd ..
echo "  Producers started."

echo "=== 5. Starting Dashboard ==="
export KAFKA_BOOTSTRAP_SERVERS=localhost:9092
nohup streamlit run dashboard/dashboard.py > dashboard/dashboard.log 2>&1 &
echo "  Dashboard started (PID $!). Log: dashboard/dashboard.log"

echo ""
echo "=== All systems running ==="
echo "  Dashboard → http://localhost:8501"
echo "  To stop producers: pkill -f produce_"
echo "  To stop dashboard: pkill -f streamlit"
echo "  To stop Docker:    docker-compose -f docker/docker-compose.yml down"
