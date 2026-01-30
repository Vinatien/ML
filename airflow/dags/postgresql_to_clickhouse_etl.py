"""
Airflow DAG: PostgreSQL → Transform → ClickHouse ETL Pipeline

This DAG extracts transaction data from PostgreSQL, transforms it with feature engineering,
and loads it into ClickHouse analytics database.

Schedule: Manual trigger with configurable date range
Author: VinaTien ML Team

Usage:
  Set the date range parameters in Airflow UI when triggering:
  - start_date: Beginning of date range (format: YYYY-MM-DD)
  - end_date: End of date range (format: YYYY-MM-DD)
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.utils.dates import days_ago
from airflow.models import Variable
import sys
from pathlib import Path
import os

# Add project paths for imports
# In Docker, /opt/airflow is the base directory
project_root = Path('/opt/airflow')
config_path = project_root / 'config'
sys.path.insert(0, str(project_root))
sys.path.insert(0, str(config_path.parent))

# Default arguments
default_args = {
    'owner': 'vinatien_ml',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
    'execution_timeout': timedelta(minutes=30),
}

# Create DAG
dag = DAG(
    'postgresql_to_clickhouse_etl',
    default_args=default_args,
    description='Extract data from PostgreSQL, transform, and load to ClickHouse (Manual trigger with date range)',
    schedule_interval=None,  # Manual trigger only
    start_date=days_ago(1),
    catchup=False,
    tags=['etl', 'postgresql', 'clickhouse', 'analytics', 'batch'],
    # DAG-level parameters that can be set when triggering
    params={
        'start_date': '2019-01-01',  # Default start date (YYYY-MM-DD)
        'end_date': '2025-12-31',    # Default end date (YYYY-MM-DD)
    },
)


def check_postgresql_connection():
    """Check if PostgreSQL is available."""
    from config.database import test_connection
    
    print("🔍 Checking PostgreSQL connection...")
    if not test_connection():
        raise Exception("PostgreSQL connection failed!")
    print("✅ PostgreSQL connection successful")


def check_clickhouse_connection():
    """Check if ClickHouse is available."""
    from config.clickhouse import test_connection
    
    print("🔍 Checking ClickHouse connection...")
    if not test_connection():
        raise Exception("ClickHouse connection failed!")
    print("✅ ClickHouse connection successful")


def extract_from_postgresql(**context):
    """Extract transaction data from PostgreSQL based on date range."""
    import pandas as pd
    from config.database import execute_query
    
    # Get date range parameters from DAG config
    params = context['params']
    start_date = params.get('start_date', '2024-01-01')
    end_date = params.get('end_date', '2024-12-31')
    
    print(f"📥 Extracting data from PostgreSQL...")
    print(f"   Date Range: {start_date} to {end_date}")
    
    query = f"""
    SELECT 
        t.id,
        t.bank_account_id,
        t.booking_date,
        t.value_date,
        t.amount,
        t.currency,
        t.booking_status as status,
        t.creditor_name,
        t.debtor_name,
        t.creditor_account_last4,
        t.debtor_account_last4,
        t.created_at,
        ba.iban,
        ba.bank_provider,
        ba.consent_status
    FROM transactions t
    LEFT JOIN bank_accounts ba ON t.bank_account_id = ba.id
    WHERE t.booking_status = 'booked'
      AND t.booking_date >= '{start_date}'
      AND t.booking_date <= '{end_date}'
    ORDER BY t.booking_date DESC
    """
    
    df = execute_query(query)
    
    if len(df) == 0:
        print(f"⚠️  No data found in PostgreSQL for date range {start_date} to {end_date}")
        return {"row_count": 0, "status": "no_data"}
    
    print(f"✅ Extracted {len(df)} transactions")
    print(f"   Actual Date Range: {df['booking_date'].min()} to {df['booking_date'].max()}")
    
    # Save to temporary location for next task
    temp_path = Path("/tmp/airflow_etl_data.parquet")
    df.to_parquet(temp_path, index=False)
    
    # Push metadata to XCom
    context['ti'].xcom_push(key='extract_count', value=len(df))
    context['ti'].xcom_push(key='date_range', value={
        'min': str(df['booking_date'].min()),
        'max': str(df['booking_date'].max())
    })
    context['ti'].xcom_push(key='requested_date_range', value={
        'start_date': start_date,
        'end_date': end_date
    })
    
    return {"row_count": len(df), "status": "success"}


def transform_data(**context):
    """Transform data and add features."""
    import pandas as pd
    from datetime import datetime
    import uuid
    
    print("🔄 Transforming data...")
    
    # Read from temp location
    temp_path = Path("/tmp/airflow_etl_data.parquet")
    if not temp_path.exists():
        raise Exception("Extracted data not found!")
    
    df = pd.read_parquet(temp_path)
    
    # Generate batch ID
    batch_id = f"batch_{datetime.now().strftime('%Y%m%d_%H%M%S')}_{uuid.uuid4().hex[:8]}"
    
    # Time-based features
    print("   → Adding time-based features...")
    df['day_of_week'] = pd.to_datetime(df['booking_date']).dt.dayofweek
    df['month'] = pd.to_datetime(df['booking_date']).dt.month
    df['year'] = pd.to_datetime(df['booking_date']).dt.year
    df['day_name'] = pd.to_datetime(df['booking_date']).dt.day_name()
    df['month_name'] = pd.to_datetime(df['booking_date']).dt.month_name()
    df['is_weekend'] = df['day_of_week'].isin([5, 6]).astype(int)
    
    # Hour of day
    if 'created_at' in df.columns:
        df['hour_of_day'] = pd.to_datetime(df['created_at']).dt.hour
    else:
        df['hour_of_day'] = 0
    
    # Transaction type features
    print("   → Adding transaction type features...")
    df['is_credit'] = (df['amount'] > 0).astype(int)
    df['is_debit'] = (df['amount'] < 0).astype(int)
    df['abs_amount'] = df['amount'].abs()
    
    # ETL metadata
    print("   → Adding ETL metadata...")
    df['etl_loaded_at'] = datetime.now()
    df['etl_batch_id'] = batch_id
    
    # Handle nulls for ClickHouse
    df['creditor_name'] = df['creditor_name'].fillna('')
    df['debtor_name'] = df['debtor_name'].fillna('')
    df['creditor_account_last4'] = df['creditor_account_last4'].fillna('')
    df['debtor_account_last4'] = df['debtor_account_last4'].fillna('')
    df['iban'] = df['iban'].fillna('')
    df['consent_status'] = df['consent_status'].fillna('')
    
    print(f"✅ Transformation complete: {len(df.columns)} features")
    
    # Save transformed data
    transformed_path = Path("/tmp/airflow_etl_transformed.parquet")
    df.to_parquet(transformed_path, index=False)
    
    # Push metadata
    context['ti'].xcom_push(key='transform_count', value=len(df))
    context['ti'].xcom_push(key='feature_count', value=len(df.columns))
    context['ti'].xcom_push(key='batch_id', value=batch_id)
    
    return {"row_count": len(df), "feature_count": len(df.columns), "batch_id": batch_id}


def save_to_feature_store(**context):
    """Save transformed data to Parquet feature store."""
    import pandas as pd
    from datetime import datetime
    from pathlib import Path
    
    print("💾 Saving to Parquet feature store...")
    
    # Read transformed data
    transformed_path = Path("/tmp/airflow_etl_transformed.parquet")
    df = pd.read_parquet(transformed_path)
    
    # Feature store directory (in Docker: /opt/airflow/data)
    feature_store_dir = Path('/opt/airflow/data/feature_store')
    feature_store_dir.mkdir(parents=True, exist_ok=True)
    
    # Save versioned file
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    versioned_file = feature_store_dir / f'transactions_features_{timestamp}.parquet'
    df.to_parquet(versioned_file, index=False)
    print(f"✅ Saved versioned: {versioned_file.name}")
    
    # Save latest file
    latest_file = feature_store_dir / 'transactions_features_latest.parquet'
    df.to_parquet(latest_file, index=False)
    print(f"✅ Saved latest: {latest_file.name}")
    
    return {"versioned_file": str(versioned_file), "latest_file": str(latest_file)}


def load_to_clickhouse(**context):
    """Load data to ClickHouse analytics database."""
    import pandas as pd
    from config.clickhouse import insert_dataframe, get_table_count
    
    print("🗄️  Loading to ClickHouse...")
    
    # Read transformed data
    transformed_path = Path("/tmp/airflow_etl_transformed.parquet")
    df = pd.read_parquet(transformed_path)
    
    # Prepare for ClickHouse
    print("   → Preparing data for ClickHouse...")
    ch_df = df.copy()
    
    # Convert datetime columns
    ch_df['booking_date'] = pd.to_datetime(ch_df['booking_date'])
    ch_df['value_date'] = pd.to_datetime(ch_df['value_date']).fillna(ch_df['booking_date'])
    ch_df['created_at'] = pd.to_datetime(ch_df['created_at'])
    ch_df['etl_loaded_at'] = pd.to_datetime(ch_df['etl_loaded_at'])
    
    # Reorder columns to match schema
    column_order = [
        'id', 'bank_account_id', 'booking_date', 'value_date', 'amount', 'currency',
        'status', 'creditor_name', 'debtor_name', 'creditor_account_last4', 
        'debtor_account_last4', 'created_at', 'iban', 'bank_provider', 'consent_status',
        'day_of_week', 'month', 'year', 'hour_of_day', 'day_name', 'month_name',
        'is_weekend', 'is_credit', 'is_debit', 'abs_amount', 'etl_loaded_at', 'etl_batch_id'
    ]
    ch_df = ch_df[column_order]
    
    # Insert to ClickHouse
    print("   → Inserting data into ClickHouse...")
    rows_inserted = insert_dataframe(
        ch_df, 
        table='transactions_fact',
        database='vinatien_analytics'
    )
    
    print(f"✅ Successfully loaded {rows_inserted} rows to ClickHouse")
    
    # Get total count
    total_count = get_table_count('vinatien_analytics.transactions_fact')
    print(f"   Total rows in ClickHouse: {total_count:,}")
    
    # Push metrics
    context['ti'].xcom_push(key='clickhouse_inserted', value=rows_inserted)
    context['ti'].xcom_push(key='clickhouse_total', value=total_count)
    
    return {"rows_inserted": rows_inserted, "total_rows": total_count}


def validate_and_report(**context):
    """Validate data and generate report."""
    from datetime import datetime
    from pathlib import Path
    
    print("✅ VALIDATION & REPORTING")
    print("=" * 70)
    
    # Get metrics from previous tasks
    ti = context['ti']
    params = context['params']
    
    extract_count = ti.xcom_pull(task_ids='extract_from_postgresql', key='extract_count') or 0
    date_range = ti.xcom_pull(task_ids='extract_from_postgresql', key='date_range') or {'min': 'N/A', 'max': 'N/A'}
    requested_range = ti.xcom_pull(task_ids='extract_from_postgresql', key='requested_date_range') or {}
    transform_count = ti.xcom_pull(task_ids='transform_data', key='transform_count') or 0
    feature_count = ti.xcom_pull(task_ids='transform_data', key='feature_count') or 0
    batch_id = ti.xcom_pull(task_ids='transform_data', key='batch_id') or 'N/A'
    clickhouse_inserted = ti.xcom_pull(task_ids='load_to_clickhouse', key='clickhouse_inserted') or 0
    clickhouse_total = ti.xcom_pull(task_ids='load_to_clickhouse', key='clickhouse_total') or 0
    
    # Format date ranges safely
    date_range_str = f"{date_range.get('min', 'N/A')} to {date_range.get('max', 'N/A')}" if isinstance(date_range, dict) else 'N/A'
    requested_range_str = f"{requested_range.get('start_date', params.get('start_date', 'N/A'))} to {requested_range.get('end_date', params.get('end_date', 'N/A'))}"
    
    report = f"""
📊 ETL Pipeline Execution Report (Batch Job)
{'=' * 70}

Execution Time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}
Batch ID: {batch_id}

Date Range Configuration:
├─ Requested Range: {requested_range_str}
└─ Actual Data Range: {date_range_str}

Source (PostgreSQL):
├─ Records Extracted: {extract_count:,}
└─ Booking Status: booked

Transformation:
├─ Records Processed: {transform_count:,}
└─ Features Created: {feature_count}

Destinations:
├─ Parquet Feature Store: ✅ Saved
└─ ClickHouse Analytics DB: {clickhouse_inserted:,} rows inserted

ClickHouse Status:
└─ Total Rows: {clickhouse_total:,}

Status: ✅ SUCCESS
{'=' * 70}
    """
    
    print(report)
    
    # Save report to logs (in Docker: /opt/airflow/logs)
    log_dir = Path('/opt/airflow/logs')
    log_dir.mkdir(parents=True, exist_ok=True)
    log_file = log_dir / f"airflow_etl_report_{datetime.now().strftime('%Y%m%d_%H%M%S')}.txt"
    
    with open(log_file, 'w') as f:
        f.write(report)
    
    print(f"\n📝 Report saved to: {log_file}")
    
    return {
        "status": "success",
        "extract_count": extract_count,
        "clickhouse_inserted": clickhouse_inserted,
        "batch_id": batch_id
    }


def cleanup_temp_files():
    """Clean up temporary files."""
    from pathlib import Path
    
    print("🧹 Cleaning up temporary files...")
    
    temp_files = [
        Path("/tmp/airflow_etl_data.parquet"),
        Path("/tmp/airflow_etl_transformed.parquet")
    ]
    
    for temp_file in temp_files:
        if temp_file.exists():
            temp_file.unlink()
            print(f"   Deleted: {temp_file.name}")
    
    print("✅ Cleanup complete")


# Define tasks
check_pg = PythonOperator(
    task_id='check_postgresql_connection',
    python_callable=check_postgresql_connection,
    dag=dag,
)

check_ch = PythonOperator(
    task_id='check_clickhouse_connection',
    python_callable=check_clickhouse_connection,
    dag=dag,
)

extract = PythonOperator(
    task_id='extract_from_postgresql',
    python_callable=extract_from_postgresql,
    provide_context=True,
    dag=dag,
)

transform = PythonOperator(
    task_id='transform_data',
    python_callable=transform_data,
    provide_context=True,
    dag=dag,
)

save_parquet = PythonOperator(
    task_id='save_to_feature_store',
    python_callable=save_to_feature_store,
    provide_context=True,
    dag=dag,
)

load_ch = PythonOperator(
    task_id='load_to_clickhouse',
    python_callable=load_to_clickhouse,
    provide_context=True,
    dag=dag,
)

validate = PythonOperator(
    task_id='validate_and_report',
    python_callable=validate_and_report,
    provide_context=True,
    dag=dag,
)

cleanup = PythonOperator(
    task_id='cleanup_temp_files',
    python_callable=cleanup_temp_files,
    trigger_rule='all_done',  # Run even if previous tasks fail
    dag=dag,
)

# Define task dependencies
[check_pg, check_ch] >> extract >> transform >> [save_parquet, load_ch] >> validate >> cleanup

# Task documentation
dag.doc_md = """
# PostgreSQL to ClickHouse ETL Pipeline

## Overview
This DAG orchestrates the complete ETL pipeline:
1. **Extract**: Pull transaction data from PostgreSQL
2. **Transform**: Add 27+ features (time-based, transaction types, metadata)
3. **Load**: Save to Parquet feature store AND ClickHouse analytics DB
4. **Validate**: Generate execution report

## Schedule
- **Frequency**: Daily at 1:00 AM
- **Timezone**: Server timezone
- **Catchup**: Disabled

## Dependencies
- PostgreSQL database (vinatien_db)
- ClickHouse database (vinatien_analytics)
- Python packages: pandas, clickhouse-driver, psycopg2-binary

## Monitoring
- Check logs in Airflow UI
- View reports in `ML/logs/airflow_etl_report_*.txt`
- Query ClickHouse: `SELECT count() FROM vinatien_analytics.transactions_fact`

## Alerting
- Retries: 3 attempts with 5-minute delay
- Timeout: 30 minutes
- Email alerts: Disabled (configure if needed)

## Manual Trigger
You can trigger this DAG manually from the Airflow UI for ad-hoc runs.
"""
