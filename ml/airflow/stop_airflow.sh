#!/bin/bash

# Stop Airflow services

echo "Stopping Airflow services..."

if [ -f "airflow_pids.txt" ]; then
    while read pid; do
        echo "Killing process $pid"
        kill $pid 2>/dev/null
    done < airflow_pids.txt
    rm airflow_pids.txt
    echo "✅ Airflow stopped"
else
    echo "No PID file found. Trying to find Airflow processes..."
    pkill -f "airflow webserver"
    pkill -f "airflow scheduler"
    echo "✅ Sent kill signals to Airflow processes"
fi
