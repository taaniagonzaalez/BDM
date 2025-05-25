#!/bin/bash
cd "$(dirname "$0")"

# --- Start Kafka stack first ---
echo "Starting Kafka stack..."
(cd kafka && ./start_kafka_stack.sh)
echo "Kafka stack started."

# --- Create shared network if it doesn't exist ---
NETWORK_NAME="airflow-minio-net"
if ! docker network ls | grep -q "$NETWORK_NAME"; then
  echo "Creating shared Docker network: $NETWORK_NAME"
  docker network create $NETWORK_NAME
else
  echo "Shared Docker network $NETWORK_NAME already exists"
fi

# --- Clean up Airflow containers, volumes, and orphans ---
echo "Cleaning up previous Airflow containers, volumes and orphans..."
docker compose -f docker-compose.airflow.yaml down --volumes --remove-orphans

# --- Build all Airflow Docker images ---
echo "Building all Docker images..."
docker compose -f docker-compose.airflow.yaml build

# --- Start postgres and redis ---
echo "Starting postgres and redis..."
docker compose -f docker-compose.airflow.yaml up -d postgres redis

echo "Waiting 30 seconds for postgres to fully initialize..."
sleep 30

# --- Initialize Airflow DB and create user ---
echo "Starting airflow-init..."
docker compose -f docker-compose.airflow.yaml up -d airflow-init 

echo "Waiting 30 seconds for Airflow DB to initialize..."
sleep 30

# --- Start Airflow worker and scheduler ---
echo "Starting airflow-scheduler and airflow-worker..."
docker compose -f docker-compose.airflow.yaml up -d airflow-scheduler airflow-worker

# --- Start MinIO cluster ---
echo "Starting MinIO cluster..."
cd minio-cluster/
docker compose -f docker-compose-cluster.yml up -d
cd ..

echo "Waiting 10 seconds for MinIO cluster to initialize..."
sleep 10

# --- Start Airflow webserver in foreground ---
echo "Starting airflow webserver (foreground)..."
docker compose -f docker-compose.airflow.yaml up airflow-webserver

echo "All services initialized!"
