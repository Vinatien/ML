"""
Direct ETL script to extract data from PostgreSQL and save to Parquet.
Run this directly without Airflow.
"""

import sys
from pathlib import Path

# Add parent directory to path
sys.path.insert(0, str(Path(__file__).parent.parent))

from config.database import execute_query
import pandas as pd
from datetime import datetime

# Create output directory
FEATURE_STORE_DIR = Path(__file__).parent.parent / 'data' / 'feature_store'
FEATURE_STORE_DIR.mkdir(parents=True, exist_ok=True)

print("🚀 Starting ETL Pipeline...")

# Step 1: Extract from PostgreSQL
print("\n📥 Step 1: Extracting data from PostgreSQL...")
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
    df = execute_query(query)
    print(f"✅ Extracted {len(df)} transactions")
except Exception as e:
    print(f"❌ Error extracting data: {e}")
    sys.exit(1)

if len(df) == 0:
    print("⚠️  No data found. Exiting.")
    sys.exit(0)

# Step 2: Transform data
print("\n🔄 Step 2: Transforming data...")

# Add derived features
df['day_of_week'] = pd.to_datetime(df['booking_date']).dt.dayofweek
df['month'] = pd.to_datetime(df['booking_date']).dt.month
df['year'] = pd.to_datetime(df['booking_date']).dt.year
df['is_credit'] = df['amount'] > 0
df['is_debit'] = df['amount'] < 0
df['abs_amount'] = df['amount'].abs()

# Add hour if datetime exists
if 'created_at' in df.columns:
    df['hour_of_day'] = pd.to_datetime(df['created_at']).dt.hour

# Add day name and month name
df['day_name'] = pd.to_datetime(df['booking_date']).dt.day_name()
df['month_name'] = pd.to_datetime(df['booking_date']).dt.month_name()

# Add is_weekend flag
df['is_weekend'] = df['day_of_week'].isin([5, 6])

print(f"✅ Added {len(df.columns)} features")
print(f"   Sample features: {list(df.columns[:10])}...")

# Step 3: Load to Feature Store
print("\n💾 Step 3: Saving to Feature Store...")

# Save versioned file
timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
versioned_file = FEATURE_STORE_DIR / f'transactions_features_{timestamp}.parquet'
df.to_parquet(versioned_file, index=False)
print(f"✅ Saved versioned file: {versioned_file.name}")

# Save latest file (overwrite)
latest_file = FEATURE_STORE_DIR / 'transactions_features_latest.parquet'
df.to_parquet(latest_file, index=False)
print(f"✅ Saved latest file: {latest_file.name}")

# Step 4: Validate
print("\n✅ Step 4: Validation Report")
print("=" * 60)
print(f"   Total records: {len(df):,}")
print(f"   Date range: {df['booking_date'].min()} to {df['booking_date'].max()}")
print(f"   Total credit: ${df[df['is_credit']]['amount'].sum():,.2f}")
print(f"   Total debit: ${df[df['is_debit']]['amount'].sum():,.2f}")
print(f"   Net amount: ${df['amount'].sum():,.2f}")
print(f"   Number of accounts: {df['bank_account_id'].nunique()}")
print(f"   Number of features: {len(df.columns)}")
print("=" * 60)

print("\n🎉 ETL Pipeline completed successfully!")
print(f"\n📊 Feature Store Location: {FEATURE_STORE_DIR.absolute()}")
print(f"📁 Files:")
print(f"   - {versioned_file.name}")
print(f"   - {latest_file.name}")
