#!/bin/bash

# Configuration
DOCKER_COMPOSE="docker-compose -f docker/docker-compose.yml"
JM_CONTAINER="docker-flink-jobmanager-1"
KAFKA_CONTAINER="docker-kafka-1"

echo "=== 1. Building & Starting Infrastructure (ARM64 Optimized) ==="
$DOCKER_COMPOSE build
$DOCKER_COMPOSE up -d
echo "Waiting 5s for services to stabilize..."
sleep 5

echo "=== 2. Creating Kafka Topics ==="
docker exec $KAFKA_CONTAINER kafka-topics --create --topic energy_data --bootstrap-server localhost:9092 --partitions 1 --replication-factor 1 --if-not-exists
docker exec $KAFKA_CONTAINER kafka-topics --create --topic cars_real --bootstrap-server localhost:9092 --partitions 1 --replication-factor 1 --if-not-exists
docker exec $KAFKA_CONTAINER kafka-topics --create --topic station_events --bootstrap-server localhost:9092 --partitions 1 --replication-factor 1 --if-not-exists
docker exec $KAFKA_CONTAINER kafka-topics --create --topic charging_commands --bootstrap-server localhost:9092 --partitions 1 --replication-factor 1 --if-not-exists

echo "=== 3. Submitting Flink Job ==="
# Note: The project directory is mounted to /opt/flink/project via docker-compose volumes
docker exec $JM_CONTAINER flink run -pyfs /opt/flink/project/optimizers -py /opt/flink/project/flink_job/process_stream.py
