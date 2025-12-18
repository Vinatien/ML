# Airflow ETL Pipeline

This directory contains the Airflow setup for ETL pipelines that extract data from PostgreSQL, transform it, and load it to the Feature Store.

## Architecture Flow

```
PostgreSQL (Backend)
    ↓
Airflow ETL Pipeline
    ├─ Extract: Fetch data from PostgreSQL
    ├─ Transform: Feature engineering & cleaning
    └─ Load: Save to Parquet Feature Store
    ↓
Feature Store (Parquet files)
    ↓
ML Training Pipeline
    ↓
Model Deployment
    ↓
ClickHouse (OLAP - Future)
```

## Setup Instructions

### 1. Install Airflow

```bash
cd ml/airflow
pip install apache-airflow==2.8.0
pip install apache-airflow-providers-postgres
```

### 2. Initialize Airflow

```bash
python setup_airflow.py
```

This will:
- Create `airflow_db` database in your PostgreSQL container
- Initialize Airflow metadata database
- Create admin user (username: `admin`, password: `admin`)

### 3. Start Airflow

```bash
# Make scripts executable
chmod +x start_airflow.sh stop_airflow.sh

# Start Airflow services
./start_airflow.sh
```

Or manually:
```bash
# Terminal 1: Start webserver
airflow webserver --port 8080

# Terminal 2: Start scheduler
airflow scheduler
```

### 4. Access Airflow UI

Open browser: http://localhost:8080

Login:
- Username: `admin`
- Password: `admin`

### 5. Enable and Run DAG

1. Find the DAG: `postgresql_to_feature_store_etl`
2. Toggle it ON
3. Click "Trigger DAG" to run manually
4. Or wait for scheduled run (daily at midnight)

## DAG Structure

### `etl_postgres_to_parquet.py`

Main ETL DAG with 4 tasks:

1. **Extract from PostgreSQL**
   - Connects to `vinatien_db` database
   - Fetches all transaction records
   - Saves raw data to temp location

2. **Transform Data**
   - Performs feature engineering
   - Filters booked transactions
   - Adds derived features (day_of_week, month, is_credit, etc.)

3. **Load to Feature Store**
   - Saves to versioned Parquet files
   - Creates `transactions_features_YYYYMMDD_HHMMSS.parquet`
   - Maintains `transactions_features_latest.parquet`

4. **Validate Data**
   - Checks data quality
   - Validates schema
   - Logs statistics

## Feature Store Location

```
ml/data/feature_store/
├── transactions_features_20251218_100000.parquet  # Versioned
├── transactions_features_20251218_150000.parquet  # Versioned
└── transactions_features_latest.parquet           # Always latest
```

## AWS Lambda Trigger (Optional)

For production deployment with AWS Lambda:

### 1. Deploy Lambda Function

```bash
# Package the lambda function
cd ml/airflow
zip lambda_function.zip aws_lambda_trigger.py

# Upload to AWS Lambda via AWS Console or CLI
```

### 2. Set Environment Variables

In AWS Lambda console:
```
AIRFLOW_URL=https://your-airflow-instance.com
AIRFLOW_USERNAME=admin
AIRFLOW_PASSWORD=your-password
```

### 3. Create EventBridge Rule

Schedule the Lambda to run daily:
```
Rate: rate(1 day)
or
Cron: cron(0 0 * * ? *)  # Daily at midnight UTC
```

### 4. Test Lambda

```python
# Test event
{
    "dag_id": "postgresql_to_feature_store_etl",
    "conf": {}
}
```

## Configuration

### Database Connection

The ETL pipeline connects to PostgreSQL using settings from `ml/config/database.py`:

```python
DB_HOST = "localhost"
DB_PORT = "5432"
DB_NAME = "vinatien_db"
DB_USER = "postgres"
DB_PASSWORD = "root"
```

### Schedule

Default schedule: `@daily` (runs every day at midnight)

To change schedule, edit `etl_postgres_to_parquet.py`:
```python
schedule_interval='@hourly'  # Every hour
# or
schedule_interval='0 */6 * * *'  # Every 6 hours
# or
schedule_interval=None  # Manual trigger only
```

## Monitoring

### View DAG Logs

In Airflow UI:
1. Click on DAG name
2. Click on task
3. Click "Log" button

### Check Feature Store

```python
import pandas as pd

# Load latest features
df = pd.read_parquet('ml/data/feature_store/transactions_features_latest.parquet')
print(df.head())
print(df.info())
```

### Monitor DAG Runs

```bash
# List DAG runs
airflow dags list-runs -d postgresql_to_feature_store_etl

# Check task instance status
airflow tasks list postgresql_to_feature_store_etl
```

## Troubleshooting

### DAG not appearing in UI

1. Check DAG file syntax:
   ```bash
   python dags/etl_postgres_to_parquet.py
   ```

2. Check Airflow logs:
   ```bash
   tail -f logs/scheduler/latest/*.log
   ```

### Database connection issues

1. Ensure PostgreSQL container is running:
   ```bash
   docker ps | grep postgres
   ```

2. Test connection from Python:
   ```python
   from ml.config.database import test_connection
   test_connection()
   ```

### Task failures

1. Check task logs in Airflow UI
2. Verify dependencies are installed:
   ```bash
   pip list | grep -E "(pandas|psycopg2|pyarrow)"
   ```

## Next Steps

After ETL pipeline is running:

1. **Feature Engineering**: Add more features in `prepare_ml_dataset()`
2. **Model Training**: Create DAG for model training using Feature Store data
3. **Model Deployment**: Create DAG for model deployment
4. **ClickHouse Integration**: Add load step to push aggregated data to ClickHouse

## File Structure

```
ml/airflow/
├── README.md                    # This file
├── setup_airflow.py            # One-time setup script
├── start_airflow.sh            # Start Airflow services
├── stop_airflow.sh             # Stop Airflow services
├── airflow_config.py           # Airflow configuration
├── aws_lambda_trigger.py       # AWS Lambda trigger function
├── dags/
│   └── etl_postgres_to_parquet.py  # Main ETL DAG
├── logs/                       # Airflow logs (auto-created)
└── plugins/                    # Custom Airflow plugins (optional)
```
