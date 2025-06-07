#!/bin/bash
cd "$(dirname "$0")"
echo "Cleaning up previous containers, volumes and orphans..."
docker compose -f docker-compose-cluster.yml down --volumes --remove-orphans

echo "Building all Docker images..."
docker compose -f docker-compose-cluster.yml build

echo "Starting MinIO cluster..."
docker compose -f docker-compose-cluster.yml up -d
