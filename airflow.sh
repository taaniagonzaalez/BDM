#!/bin/bash

echo "Cleaning up previous containers, volumes and orphans..."
docker compose -f docker-compose.airflow.yaml down --volumes --remove-orphans

echo "Building all Docker images..."
docker compose -f docker-compose.airflow.yaml build

echo "Starting postgres and redis..."
docker compose -f docker-compose.airflow.yaml up -d postgres redis

echo "Waiting 30 seconds for postgres to fully initialize..."
sleep 30

echo "Starting airflow components..."
docker compose -f docker-compose.airflow.yaml up -d airflow-init 

echo "Waiting 30 seconds for postgres to fully initialize..."
sleep 30

echo "Starting airflow components..."
docker compose -f docker-compose.airflow.yaml up -d airflow-scheduler airflow-worker

echo "Waiting 30 seconds for airflow to fully initialize..."
sleep 30

echo "Starting airflow webserver (foreground)..."
docker compose -f docker-compose.airflow.yaml up airflow-webserver

echo "All services initialized!"
