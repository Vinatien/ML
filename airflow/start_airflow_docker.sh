#!/bin/bash

# VinaTien Airflow Docker Startup Script
# This script initializes and starts Apache Airflow with Docker Compose

echo "🚀 Starting VinaTien Airflow Environment with Docker..."

# Set Airflow UID
export AIRFLOW_UID=$(id -u)

# Create necessary directories
mkdir -p logs plugins dags

# Check if PostgreSQL and ClickHouse are running
echo "📊 Checking database connections..."

# Check PostgreSQL
if ! docker ps | grep -q vinatien-postgres; then
    echo "⚠️  Warning: PostgreSQL container (vinatien-postgres) is not running"
    echo "   Please start it first: cd ../backend && docker-compose up -d"
fi

# Check ClickHouse
if ! docker ps | grep -q vinatien-clickhouse; then
    echo "⚠️  Warning: ClickHouse container (vinatien-clickhouse) is not running"
    echo "   Please start it first: cd ../clickhouse && docker-compose up -d"
fi

echo ""
echo "🏗️  Initializing Airflow (first time only)..."
docker-compose -f docker-compose-airflow.yaml up airflow-init

echo ""
echo "▶️  Starting Airflow services..."
docker-compose -f docker-compose-airflow.yaml up -d

echo ""
echo "⏳ Waiting for Airflow to be ready (this may take 30-60 seconds)..."
sleep 30

echo ""
echo "✅ Airflow is starting up!"
echo ""
echo "📋 Access Information:"
echo "   Web UI: http://localhost:8080"
echo "   Username: admin"
echo "   Password: admin"
echo ""
echo "📂 Your DAG is located at: ./dags/postgresql_to_clickhouse_etl.py"
echo ""
echo "🔍 Monitor logs with:"
echo "   docker-compose -f docker-compose-airflow.yaml logs -f airflow-webserver"
echo "   docker-compose -f docker-compose-airflow.yaml logs -f airflow-scheduler"
echo ""
echo "🛑 To stop Airflow:"
echo "   docker-compose -f docker-compose-airflow.yaml down"
echo ""
