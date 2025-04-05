
#!/bin/bash

echo "Moving to Kafka folder..."
cd "$(dirname "$0")"

echo "Cleaning up previous containers, volumes and orphans..."
docker compose -f docker-compose.kafka.yaml down --volumes --remove-orphans

echo "Building all Docker images..."
docker compose -f docker-compose.kafka.yaml build

echo "Starting Zookeeper..."
docker compose -f docker-compose.kafka.yaml up -d zookeeper

echo "Waiting 10 seconds for Zookeeper to fully initialize..."
sleep 10

echo "Starting Kafka..."
docker compose -f docker-compose.kafka.yaml up -d kafka

echo "Waiting 20 seconds for Kafka to fully initialize..."
sleep 20

echo "Creating Kafka topics..."
docker compose -f docker-compose.kafka.yaml up kafka-create-topics

echo "Starting registration producer & consumer..."
docker compose -f docker-compose.kafka.yaml up -d registration-producer registration-consumer

echo "Waiting 30 seconds for some registration events..."
sleep 30

echo "Starting search producer & consumer..."
docker compose -f docker-compose.kafka.yaml up -d search-producer search-consumer

echo "All services initialized!"