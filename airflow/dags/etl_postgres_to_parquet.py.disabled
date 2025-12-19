"""
Airflow DAG for ETL pipeline: PostgreSQL → Parquet → Feature Store
Extracts transaction data from PostgreSQL, transforms it, and loads to Parquet files.
"""

from datetime import datetime, timedelta
from pathlib import Path
import sys

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago

# Add ML directory to path
sys.path.append(str(Path(__file__).parent.parent.parent / 'ml'))

from ml.data.fetch_transactions import (
    fetch_all_transactions,
    fetch_recent_transactions,
    prepare_ml_dataset
)
from ml.data.export_transactions import export_to_parquet


# Default arguments for the DAG
default_args = {
    'owner': 'ml-team',
    'depends_on_past': False,
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
    'execution_timeout': timedelta(hours=1),
}


def extract_from_postgres(**context):
    """
    Extract transaction data from PostgreSQL database.
    
    This function connects to the PostgreSQL database (managed by backend)
    and extracts all transaction data.
    """
    print("=" * 60)
    print("EXTRACT: Fetching data from PostgreSQL...")
    print("=" * 60)
    
    # Fetch all transactions from PostgreSQL
    df = fetch_all_transactions()
    
    print(f"✅ Extracted {len(df)} records from PostgreSQL")
    print(f"Columns: {df.columns.tolist()}")
    print(f"Date range: {df['booking_date'].min()} to {df['booking_date'].max()}")
    
    # Store in XCom for next task
    context['task_instance'].xcom_push(key='raw_data_shape', value=df.shape)
    context['task_instance'].xcom_push(key='record_count', value=len(df))
    
    # Save to temporary location
    temp_path = Path('/tmp/raw_transactions.parquet')
    df.to_parquet(temp_path, index=False)
    context['task_instance'].xcom_push(key='raw_data_path', value=str(temp_path))
    
    return str(temp_path)


def transform_data(**context):
    """
    Transform raw transaction data for ML workflows.
    
    Performs:
    - Data cleaning
    - Feature engineering
    - Type conversions
    - Filtering
    """
    print("=" * 60)
    print("TRANSFORM: Processing data for ML...")
    print("=" * 60)
    
    # Get raw data path from previous task
    raw_data_path = context['task_instance'].xcom_pull(
        task_ids='extract_from_postgres',
        key='raw_data_path'
    )
    
    import pandas as pd
    df_raw = pd.read_parquet(raw_data_path)
    
    print(f"Input records: {len(df_raw)}")
    
    # Prepare ML dataset with feature engineering
    df_transformed = prepare_ml_dataset(
        include_account_info=True,
        filter_status='booked'  # Only use booked transactions for training
    )
    
    print(f"✅ Transformed to {len(df_transformed)} records with {len(df_transformed.columns)} features")
    print(f"New features: {[col for col in df_transformed.columns if col not in df_raw.columns]}")
    
    # Save transformed data
    transformed_path = Path('/tmp/transformed_transactions.parquet')
    df_transformed.to_parquet(transformed_path, index=False)
    
    context['task_instance'].xcom_push(key='transformed_data_path', value=str(transformed_path))
    context['task_instance'].xcom_push(key='transformed_record_count', value=len(df_transformed))
    
    return str(transformed_path)


def load_to_feature_store(**context):
    """
    Load transformed data to Feature Store (Parquet format).
    
    Stores data in versioned Parquet files for:
    - Model training
    - Feature serving
    - Historical analysis
    """
    print("=" * 60)
    print("LOAD: Saving to Feature Store...")
    print("=" * 60)
    
    # Get transformed data
    transformed_path = context['task_instance'].xcom_pull(
        task_ids='transform_data',
        key='transformed_data_path'
    )
    
    import pandas as pd
    df = pd.read_parquet(transformed_path)
    
    # Define feature store location
    feature_store_dir = Path(__file__).parent.parent.parent / 'ml' / 'data' / 'feature_store'
    feature_store_dir.mkdir(parents=True, exist_ok=True)
    
    # Version the data with timestamp
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    feature_path = feature_store_dir / f'transactions_features_{timestamp}.parquet'
    
    # Save to feature store
    df.to_parquet(feature_path, index=False, compression='snappy')
    
    print(f"✅ Saved {len(df)} records to feature store: {feature_path}")
    
    # Also save as "latest" for easy access
    latest_path = feature_store_dir / 'transactions_features_latest.parquet'
    df.to_parquet(latest_path, index=False, compression='snappy')
    
    print(f"✅ Updated latest features: {latest_path}")
    
    context['task_instance'].xcom_push(key='feature_store_path', value=str(feature_path))
    
    return str(feature_path)


def validate_data(**context):
    """
    Validate the ETL pipeline output.
    
    Checks:
    - Data quality
    - Schema consistency
    - Record counts
    """
    print("=" * 60)
    print("VALIDATE: Checking data quality...")
    print("=" * 60)
    
    feature_path = context['task_instance'].xcom_pull(
        task_ids='load_to_feature_store',
        key='feature_store_path'
    )
    
    import pandas as pd
    df = pd.read_parquet(feature_path)
    
    # Validation checks
    checks = {
        'total_records': len(df),
        'null_values': df.isnull().sum().to_dict(),
        'duplicate_records': df.duplicated().sum(),
        'date_range': {
            'min': str(df['booking_date'].min()),
            'max': str(df['booking_date'].max())
        },
        'amount_stats': {
            'min': float(df['amount'].min()),
            'max': float(df['amount'].max()),
            'mean': float(df['amount'].mean())
        }
    }
    
    print("✅ Validation Results:")
    print(f"  Total records: {checks['total_records']}")
    print(f"  Duplicates: {checks['duplicate_records']}")
    print(f"  Date range: {checks['date_range']['min']} to {checks['date_range']['max']}")
    print(f"  Amount range: {checks['amount_stats']['min']} to {checks['amount_stats']['max']}")
    
    # Check for critical issues
    if checks['duplicate_records'] > 0:
        print(f"⚠️  Warning: Found {checks['duplicate_records']} duplicate records")
    
    if checks['total_records'] == 0:
        raise ValueError("❌ No records in feature store!")
    
    print("✅ All validations passed!")
    
    return checks


# Define the DAG
dag = DAG(
    'postgresql_to_feature_store_etl',
    default_args=default_args,
    description='ETL pipeline: PostgreSQL → Parquet Feature Store',
    schedule_interval='@daily',  # Run daily at midnight
    start_date=days_ago(1),
    catchup=False,
    tags=['etl', 'ml', 'feature-store'],
)

# Define tasks
extract_task = PythonOperator(
    task_id='extract_from_postgres',
    python_callable=extract_from_postgres,
    provide_context=True,
    dag=dag,
)

transform_task = PythonOperator(
    task_id='transform_data',
    python_callable=transform_data,
    provide_context=True,
    dag=dag,
)

load_task = PythonOperator(
    task_id='load_to_feature_store',
    python_callable=load_to_feature_store,
    provide_context=True,
    dag=dag,
)

validate_task = PythonOperator(
    task_id='validate_data',
    python_callable=validate_data,
    provide_context=True,
    dag=dag,
)

# Define task dependencies (DAG flow)
extract_task >> transform_task >> load_task >> validate_task
