#!/bin/bash

# Start Airflow Webserver and Scheduler
# This script starts both services in the background

echo "╔═══════════════════════════════════════════════════════════╗"
echo "║          Starting Airflow Services                        ║"
echo "╚═══════════════════════════════════════════════════════════╝"

# Load Airflow configuration
export PYTHONPATH="${PYTHONPATH}:$(pwd)"
python airflow_config.py

echo ""
echo "📦 Starting Airflow Webserver on port 8080..."
airflow webserver --port 8080 > logs/webserver.log 2>&1 &
WEBSERVER_PID=$!
echo "✅ Webserver started (PID: $WEBSERVER_PID)"

echo ""
echo "⏰ Starting Airflow Scheduler..."
airflow scheduler > logs/scheduler.log 2>&1 &
SCHEDULER_PID=$!
echo "✅ Scheduler started (PID: $SCHEDULER_PID)"

echo ""
echo "╔═══════════════════════════════════════════════════════════╗"
echo "║             Airflow is now running! 🎉                    ║"
echo "╚═══════════════════════════════════════════════════════════╝"
echo ""
echo "🌐 Airflow UI: http://localhost:8080"
echo "👤 Username: admin"
echo "🔑 Password: admin"
echo ""
echo "📝 Logs:"
echo "   Webserver: $(pwd)/logs/webserver.log"
echo "   Scheduler: $(pwd)/logs/scheduler.log"
echo ""
echo "To stop Airflow:"
echo "   kill $WEBSERVER_PID $SCHEDULER_PID"
echo ""
echo "PIDs saved to airflow_pids.txt"
echo "$WEBSERVER_PID" > airflow_pids.txt
echo "$SCHEDULER_PID" >> airflow_pids.txt
