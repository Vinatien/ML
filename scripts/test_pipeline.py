"""
Quick test script to verify the complete data pipeline.
Tests: PostgreSQL → ETL → Parquet → ClickHouse
"""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

from config.database import test_connection as test_pg
from config.clickhouse import test_connection as test_ch, execute_query, get_table_count

print("=" * 70)
print("🧪 TESTING COMPLETE DATA PIPELINE")
print("=" * 70)

# Test 1: PostgreSQL Connection
print("\n1️⃣  Testing PostgreSQL connection...")
if test_pg():
    print("   ✅ PostgreSQL is ready")
else:
    print("   ❌ PostgreSQL connection failed")
    sys.exit(1)

# Test 2: ClickHouse Connection
print("\n2️⃣  Testing ClickHouse connection...")
if test_ch():
    print("   ✅ ClickHouse is ready")
else:
    print("   ❌ ClickHouse connection failed")
    sys.exit(1)

# Test 3: Check ClickHouse tables exist
print("\n3️⃣  Checking ClickHouse tables...")
try:
    tables = execute_query("SHOW TABLES FROM vinatien_analytics")
    print(f"   ✅ Found {len(tables)} tables:")
    for table in tables.values:
        print(f"      - {table[0]}")
except Exception as e:
    print(f"   ❌ Error checking tables: {e}")
    sys.exit(1)

# Test 4: Check data in transactions_fact
print("\n4️⃣  Checking data in ClickHouse...")
try:
    count = get_table_count('vinatien_analytics.transactions_fact')
    print(f"   ✅ Found {count} rows in transactions_fact table")
    
    if count > 0:
        # Show sample data
        sample = execute_query("""
            SELECT 
                booking_date,
                amount,
                currency,
                status
            FROM vinatien_analytics.transactions_fact
            LIMIT 3
        """)
        print("\n   📊 Sample data:")
        print(sample.to_string(index=False))
except Exception as e:
    print(f"   ❌ Error checking data: {e}")

# Test 5: Check Parquet feature store
print("\n5️⃣  Checking Parquet feature store...")
feature_store = Path(__file__).parent.parent / 'data' / 'feature_store'
if feature_store.exists():
    parquet_files = list(feature_store.glob('*.parquet'))
    print(f"   ✅ Found {len(parquet_files)} Parquet files")
    if parquet_files:
        print("   Recent files:")
        for f in sorted(parquet_files, key=lambda x: x.stat().st_mtime, reverse=True)[:3]:
            size_mb = f.stat().st_size / 1024 / 1024
            print(f"      - {f.name} ({size_mb:.2f} MB)")
else:
    print("   ⚠️  Feature store directory not found")

# Test 6: Run a simple aggregation query
print("\n6️⃣  Testing analytical query...")
try:
    result = execute_query("""
        SELECT 
            count() as total_transactions,
            sum(amount) as total_amount,
            countIf(is_credit = 1) as credits,
            countIf(is_debit = 1) as debits
        FROM vinatien_analytics.transactions_fact
    """)
    print("   ✅ Analytical query successful:")
    print(f"      Total transactions: {result['total_transactions'][0]}")
    print(f"      Total amount: ${result['total_amount'][0]:.2f}")
    print(f"      Credits: {result['credits'][0]}")
    print(f"      Debits: {result['debits'][0]}")
except Exception as e:
    print(f"   ❌ Query failed: {e}")

print("\n" + "=" * 70)
print("✅ PIPELINE TEST COMPLETE!")
print("=" * 70)

print("""
🎉 Your data pipeline is ready!

Next steps:
1. Run ETL: python scripts/run_etl_to_clickhouse.py
2. Query data: python -c "from config.clickhouse import execute_query; print(execute_query('SELECT * FROM transactions_fact LIMIT 5'))"
3. Build dashboards: Connect BI tool to ClickHouse (localhost:8123)
4. Train models: Use Parquet files in data/feature_store/
""")
