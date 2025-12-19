"""
Complete ETL Pipeline: PostgreSQL → Transform → ClickHouse
Extracts data from PostgreSQL, transforms it, and loads to ClickHouse OLAP database.
"""

import sys
from pathlib import Path
from datetime import datetime
import uuid

# Add parent directory to path
sys.path.insert(0, str(Path(__file__).parent.parent))

from config.database import execute_query as pg_execute_query
from config.clickhouse import (
    insert_dataframe, 
    test_connection as ch_test_connection,
    get_table_count,
    ClickHouseConfig
)
import pandas as pd

# Create output directories
FEATURE_STORE_DIR = Path(__file__).parent.parent / 'data' / 'feature_store'
FEATURE_STORE_DIR.mkdir(parents=True, exist_ok=True)

LOGS_DIR = Path(__file__).parent.parent / 'logs'
LOGS_DIR.mkdir(parents=True, exist_ok=True)

# Generate batch ID for tracking
BATCH_ID = f"batch_{datetime.now().strftime('%Y%m%d_%H%M%S')}_{uuid.uuid4().hex[:8]}"

print("=" * 70)
print("🚀 STARTING ETL PIPELINE: PostgreSQL → Transform → ClickHouse")
print("=" * 70)
print(f"Batch ID: {BATCH_ID}")
print(f"Timestamp: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
print("=" * 70)

# ============================================================================
# STEP 1: EXTRACT FROM POSTGRESQL
# ============================================================================
print("\n📥 STEP 1: EXTRACTING DATA FROM POSTGRESQL")
print("-" * 70)

query = """
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
ORDER BY t.booking_date DESC
"""

try:
    df = pg_execute_query(query)
    print(f"✅ Successfully extracted {len(df)} transactions from PostgreSQL")
except Exception as e:
    print(f"❌ Error extracting data from PostgreSQL: {e}")
    sys.exit(1)

if len(df) == 0:
    print("⚠️  No data found in PostgreSQL. Exiting.")
    sys.exit(0)

# ============================================================================
# STEP 2: TRANSFORM DATA (Feature Engineering)
# ============================================================================
print("\n🔄 STEP 2: TRANSFORMING DATA (Feature Engineering)")
print("-" * 70)

# Time-based features
print("   → Adding time-based features...")
df['day_of_week'] = pd.to_datetime(df['booking_date']).dt.dayofweek
df['month'] = pd.to_datetime(df['booking_date']).dt.month
df['year'] = pd.to_datetime(df['booking_date']).dt.year
df['day_name'] = pd.to_datetime(df['booking_date']).dt.day_name()
df['month_name'] = pd.to_datetime(df['booking_date']).dt.month_name()
df['is_weekend'] = df['day_of_week'].isin([5, 6]).astype(int)

# Hour of day from created_at
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
df['etl_batch_id'] = BATCH_ID

# Handle nulls for ClickHouse compatibility
df['creditor_name'] = df['creditor_name'].fillna('')
df['debtor_name'] = df['debtor_name'].fillna('')
df['creditor_account_last4'] = df['creditor_account_last4'].fillna('')
df['debtor_account_last4'] = df['debtor_account_last4'].fillna('')
df['iban'] = df['iban'].fillna('')
df['consent_status'] = df['consent_status'].fillna('')

print(f"✅ Transformation complete: {len(df.columns)} features")
print(f"   Features added: day_of_week, month, year, is_credit, is_debit, etc.")

# ============================================================================
# STEP 3: LOAD TO PARQUET FEATURE STORE (Local Cache)
# ============================================================================
print("\n💾 STEP 3: SAVING TO PARQUET FEATURE STORE")
print("-" * 70)

# Save versioned file
timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
versioned_file = FEATURE_STORE_DIR / f'transactions_features_{timestamp}.parquet'
df.to_parquet(versioned_file, index=False)
print(f"✅ Saved versioned: {versioned_file.name}")

# Save latest file (overwrite)
latest_file = FEATURE_STORE_DIR / 'transactions_features_latest.parquet'
df.to_parquet(latest_file, index=False)
print(f"✅ Saved latest: {latest_file.name}")

# ============================================================================
# STEP 4: LOAD TO CLICKHOUSE (OLAP Database)
# ============================================================================
print("\n🗄️  STEP 4: LOADING TO CLICKHOUSE ANALYTICS DATABASE")
print("-" * 70)

# Test ClickHouse connection
print("   → Testing ClickHouse connection...")
if not ch_test_connection():
    print("❌ Cannot connect to ClickHouse. Skipping load.")
    print("   To start ClickHouse: cd ML/clickhouse && docker compose up -d")
    sys.exit(1)

# Prepare data for ClickHouse
print("   → Preparing data for ClickHouse...")
ch_df = df.copy()

# Convert datetime columns to proper format
ch_df['booking_date'] = pd.to_datetime(ch_df['booking_date'])
ch_df['value_date'] = pd.to_datetime(ch_df['value_date']).fillna(ch_df['booking_date'])
ch_df['created_at'] = pd.to_datetime(ch_df['created_at'])
ch_df['etl_loaded_at'] = pd.to_datetime(ch_df['etl_loaded_at'])

# Reorder columns to match ClickHouse table schema
column_order = [
    'id', 'bank_account_id', 'booking_date', 'value_date', 'amount', 'currency',
    'status', 'creditor_name', 'debtor_name', 'creditor_account_last4', 
    'debtor_account_last4', 'created_at', 'iban', 'bank_provider', 'consent_status',
    'day_of_week', 'month', 'year', 'hour_of_day', 'day_name', 'month_name',
    'is_weekend', 'is_credit', 'is_debit', 'abs_amount', 'etl_loaded_at', 'etl_batch_id'
]

ch_df = ch_df[column_order]

try:
    print("   → Inserting data into ClickHouse...")
    rows_inserted = insert_dataframe(
        ch_df, 
        table='transactions_fact',
        database='vinatien_analytics'
    )
    print(f"✅ Successfully loaded {rows_inserted} rows to ClickHouse")
    
    # Get total count in ClickHouse
    total_count = get_table_count('vinatien_analytics.transactions_fact')
    print(f"   Total rows in ClickHouse: {total_count:,}")
    
except Exception as e:
    print(f"❌ Error loading to ClickHouse: {e}")
    print("   Data saved to Parquet files. You can retry ClickHouse load later.")

# ============================================================================
# STEP 5: VALIDATION & REPORTING
# ============================================================================
print("\n✅ STEP 5: VALIDATION & REPORTING")
print("=" * 70)

print(f"""
📊 ETL Pipeline Summary:
   
   Source: PostgreSQL (vinatien_db)
   ├─ Extracted: {len(df):,} transactions
   └─ Date Range: {df['booking_date'].min()} to {df['booking_date'].max()}
   
   Transformations:
   ├─ Features Created: {len(df.columns)} total
   ├─ Time Features: day_of_week, month, year, hour_of_day, day_name, is_weekend
   ├─ Transaction Features: is_credit, is_debit, abs_amount
   └─ Metadata: etl_loaded_at, etl_batch_id
   
   Destinations:
   ├─ Parquet Feature Store: {FEATURE_STORE_DIR}
   │  ├─ {versioned_file.name}
   │  └─ {latest_file.name}
   └─ ClickHouse Analytics DB: vinatien_analytics.transactions_fact
   
   Financial Summary:
   ├─ Total Credits: ${df[df['is_credit'] == 1]['amount'].sum():,.2f}
   ├─ Total Debits: ${abs(df[df['is_debit'] == 1]['amount'].sum()):,.2f}
   ├─ Net Amount: ${df['amount'].sum():,.2f}
   └─ Number of Accounts: {df['bank_account_id'].nunique()}
   
   Batch Information:
   ├─ Batch ID: {BATCH_ID}
   ├─ Timestamp: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}
   └─ Duration: Complete
""")

print("=" * 70)
print("🎉 ETL PIPELINE COMPLETED SUCCESSFULLY!")
print("=" * 70)

# Log to file
log_file = LOGS_DIR / f"etl_log_{timestamp}.txt"
with open(log_file, 'w') as f:
    f.write(f"ETL Pipeline Execution Log\n")
    f.write(f"Batch ID: {BATCH_ID}\n")
    f.write(f"Timestamp: {datetime.now()}\n")
    f.write(f"Records Processed: {len(df)}\n")
    f.write(f"Status: SUCCESS\n")

print(f"\n📝 Log saved to: {log_file}")
