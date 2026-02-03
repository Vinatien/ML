#!/bin/bash

echo "======================================================================"
echo "  AIRFLOW DAG TRIGGER TEST"
echo "======================================================================"
echo ""

# Check Airflow is running
echo "🔍 Checking Airflow status..."
docker ps | grep vinatien-airflow-webserver > /dev/null
if [ $? -ne 0 ]; then
    echo "❌ Airflow webserver is not running"
    exit 1
fi
echo "✅ Airflow webserver is running"
echo ""

# List the DAG
echo "📋 Checking DAG status..."
docker exec vinatien-airflow-webserver airflow dags list | grep postgresql_to_clickhouse_etl
if [ $? -ne 0 ]; then
    echo "❌ ETL DAG not found"
    echo ""
    echo "Available DAGs:"
    docker exec vinatien-airflow-webserver airflow dags list
    exit 1
fi
echo "✅ ETL DAG found"
echo ""

# Check DAG state
echo "📊 DAG Details:"
docker exec vinatien-airflow-webserver airflow dags show postgresql_to_clickhouse_etl 2>/dev/null | head -20
echo ""

# Trigger the DAG with date range parameters
echo "======================================================================"
echo "  TRIGGERING DAG"
echo "======================================================================"
echo ""
echo "🚀 Triggering postgresql_to_clickhouse_etl DAG..."
echo "   Date Range: 2019-01-01 to 2025-12-31"
echo ""

docker exec vinatien-airflow-webserver airflow dags trigger \
    postgresql_to_clickhouse_etl \
    --conf '{"start_date": "2019-01-01", "end_date": "2025-12-31"}'

if [ $? -eq 0 ]; then
    echo ""
    echo "✅ DAG triggered successfully!"
    echo ""
    echo "======================================================================"
    echo "  MONITORING DAG EXECUTION"
    echo "======================================================================"
    echo ""
    echo "📊 Waiting for DAG to start (5 seconds)..."
    sleep 5
    
    echo ""
    echo "📋 Recent DAG runs:"
    docker exec vinatien-airflow-webserver airflow dags list-runs \
        --dag-id postgresql_to_clickhouse_etl \
        --state running \
        --state success \
        --state failed \
        --output table 2>/dev/null | head -20
    
    echo ""
    echo "======================================================================"
    echo "  NEXT STEPS"
    echo "======================================================================"
    echo ""
    echo "1️⃣  Monitor DAG progress:"
    echo "   docker exec vinatien-airflow-webserver airflow dags list-runs --dag-id postgresql_to_clickhouse_etl"
    echo ""
    echo "2️⃣  View DAG logs:"
    echo "   docker exec vinatien-airflow-webserver airflow tasks logs postgresql_to_clickhouse_etl extract_from_postgresql <run_id>"
    echo ""
    echo "3️⃣  Check ClickHouse data:"
    echo "   docker exec ml-clickhouse-1 clickhouse-client --database vinatien_analytics --query \"SELECT COUNT(*) FROM transactions_fact FINAL\""
    echo ""
    echo "4️⃣  View Airflow UI:"
    echo "   Open http://localhost:8080 in your browser"
    echo ""
else
    echo ""
    echo "❌ Failed to trigger DAG"
    exit 1
fi
