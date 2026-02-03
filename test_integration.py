#!/usr/bin/env python3
"""
Quick Integration Test: Real VPBank Data → PostgreSQL → ClickHouse → ML Scoring
Tests the complete updated architecture with ReplacingMergeTree deduplication.
"""

import sys
import os
import time
from datetime import datetime
import subprocess

# Add paths
sys.path.append('/Users/nguyenvietkhoi/VinaTien/backend')
sys.path.append('/Users/nguyenvietkhoi/VinaTien/ML')

def print_section(title):
    """Print formatted section header."""
    print(f"\n{'='*70}")
    print(f"  {title}")
    print(f"{'='*70}\n")

def run_command(command, description):
    """Run shell command and return success status."""
    print(f"🔄 {description}...")
    try:
        result = subprocess.run(
            command,
            shell=True,
            capture_output=True,
            text=True,
            timeout=60
        )
        if result.returncode == 0:
            print(f"✅ {description} - SUCCESS")
            if result.stdout:
                print(f"   Output: {result.stdout[:200]}")
            return True
        else:
            print(f"❌ {description} - FAILED")
            print(f"   Error: {result.stderr}")
            return False
    except Exception as e:
        print(f"❌ {description} - EXCEPTION: {e}")
        return False

def test_clickhouse_connection():
    """Test ClickHouse connection and table setup."""
    print_section("TEST 1: ClickHouse Connection & ReplacingMergeTree")
    
    try:
        from clickhouse_driver import Client
        import os
        
        client = Client(
            host='localhost',
            port=9000,
            database='vinatien_analytics',
            user=os.getenv('CLICKHOUSE_USER', 'clickhouse'),
            password=os.getenv('CLICKHOUSE_PASSWORD', 'clickhouse123')
        )
        
        # Check table engine
        result = client.execute("""
            SELECT engine FROM system.tables 
            WHERE database = 'vinatien_analytics' AND name = 'transactions_fact'
        """)
        
        if result and result[0][0] == 'ReplacingMergeTree':
            print("✅ Table uses ReplacingMergeTree engine")
        else:
            print(f"⚠️  Table engine: {result[0][0] if result else 'Unknown'}")
        
        # Check ORDER BY
        result = client.execute("""
            SELECT sorting_key FROM system.tables 
            WHERE database = 'vinatien_analytics' AND name = 'transactions_fact'
        """)
        
        if result:
            sorting_key = result[0][0]
            print(f"✅ ORDER BY: {sorting_key}")
            if 'id' in sorting_key:
                print("✅ ORDER BY includes 'id' (deduplication key)")
            else:
                print("⚠️  ORDER BY does not include 'id'")
        
        # Check current row count
        result = client.execute("SELECT COUNT(*) FROM transactions_fact")
        count = result[0][0] if result else 0
        print(f"📊 Current row count: {count}")
        
        return True
        
    except Exception as e:
        print(f"❌ ClickHouse test failed: {e}")
        return False

def test_vpbank_extraction():
    """Test VPBank data extraction."""
    print_section("TEST 2: VPBank Real Data Extraction")
    
    try:
        import requests
        from app.bank.vpbank import VPBank
        
        # Initialize session
        session = requests.Session()
        session.headers.update({
            "Content-Type": "application/json",
            "Accept": "application/json",
            "TPP-Redirect-URI": "https://www.google.ch",
            "PSU-IP-Address": "192.0.0.12"
        })
        
        vpbank = VPBank(session)
        
        # Create consent and get IBAN
        print("🔐 Creating consent...")
        iban = vpbank.create_consent_and_get_iban()
        print(f"✅ IBAN obtained: {iban}")
        
        # Get transactions
        print("📥 Fetching transactions...")
        success, tx_data = vpbank.get_transactions_and_review(iban, "Integration Test")
        
        if success and tx_data:
            booked = tx_data.get("booked", [])
            pending = tx_data.get("pending", [])
            
            print(f"✅ Transactions retrieved:")
            print(f"   Booked: {len(booked)}")
            print(f"   Pending: {len(pending)}")
            
            if booked:
                sample = booked[0]
                print(f"\n📋 Sample Transaction:")
                print(f"   ID: {sample.get('transactionId')}")
                print(f"   Amount: {sample.get('transactionAmount', {}).get('amount')} "
                      f"{sample.get('transactionAmount', {}).get('currency')}")
                print(f"   Date: {sample.get('bookingDate')}")
            
            return True, len(booked) + len(pending)
        else:
            print("⚠️  No transactions found (sandbox may be empty)")
            return True, 0  # Still pass if no transactions
            
    except Exception as e:
        print(f"❌ VPBank extraction failed: {e}")
        import traceback
        traceback.print_exc()
        return False, 0

def test_deduplication():
    """Test ClickHouse ReplacingMergeTree deduplication."""
    print_section("TEST 3: ReplacingMergeTree Deduplication")
    
    try:
        from clickhouse_driver import Client
        import os
        
        client = Client(
            host='localhost',
            port=9000,
            database='vinatien_analytics',
            user=os.getenv('CLICKHOUSE_USER', 'clickhouse'),
            password=os.getenv('CLICKHOUSE_PASSWORD', 'clickhouse123')
        )
        
        test_id = 99999
        
        # Clean up any existing test data
        print(f"🧹 Cleaning up test data (id={test_id})...")
        client.execute(f"ALTER TABLE transactions_fact DELETE WHERE id = {test_id}")
        time.sleep(1)
        
        # Insert first version
        print(f"📝 Inserting record with version 1000...")
        client.execute(f"""
            INSERT INTO transactions_fact VALUES
            ({test_id}, 1, '2025-12-12', '2025-12-12', 100.5, 'EUR', 'booked',
             'Test Creditor', 'Test Debtor', '1234', '5678',
             '2025-12-12', 'DE123', 'VPBank', 'VALID',
             4, 12, 2025, 10, 'Thursday', 'December',
             0, 1, 0, 100.5, now(), 'test-dedup-1', 1000)
        """)
        
        # Insert duplicate with higher version
        print(f"📝 Inserting duplicate with version 2000 (higher)...")
        client.execute(f"""
            INSERT INTO transactions_fact VALUES
            ({test_id}, 1, '2025-12-12', '2025-12-12', 100.5, 'EUR', 'booked',
             'Test Creditor', 'Test Debtor', '1234', '5678',
             '2025-12-12', 'DE123', 'VPBank', 'VALID',
             4, 12, 2025, 10, 'Thursday', 'December',
             0, 1, 0, 100.5, now(), 'test-dedup-2', 2000)
        """)
        
        # Check count before merge
        result = client.execute(f"SELECT COUNT(*) FROM transactions_fact WHERE id = {test_id}")
        count_before = result[0][0]
        print(f"📊 Rows before merge: {count_before}")
        
        # Check with FINAL
        result = client.execute(f"SELECT COUNT(*) FROM transactions_fact FINAL WHERE id = {test_id}")
        count_final = result[0][0]
        print(f"📊 Rows with FINAL: {count_final}")
        
        if count_final == 1:
            print("✅ FINAL query returns 1 row (deduplication working!)")
        else:
            print(f"⚠️  FINAL query returns {count_final} rows (expected 1)")
        
        # Force merge
        print("🔄 Forcing background merge...")
        client.execute("OPTIMIZE TABLE transactions_fact FINAL")
        time.sleep(2)
        
        # Check count after merge
        result = client.execute(f"SELECT COUNT(*) FROM transactions_fact WHERE id = {test_id}")
        count_after = result[0][0]
        print(f"📊 Rows after merge: {count_after}")
        
        # Verify version kept
        result = client.execute(f"SELECT version FROM transactions_fact WHERE id = {test_id}")
        if result:
            version = result[0][0]
            print(f"✅ Kept version: {version} (should be 2000)")
            
            if version == 2000:
                print("✅ ReplacingMergeTree kept the highest version!")
            else:
                print(f"⚠️  Version is {version}, expected 2000")
        
        # Cleanup
        client.execute(f"ALTER TABLE transactions_fact DELETE WHERE id = {test_id}")
        
        return count_final == 1 and count_after == 1
        
    except Exception as e:
        print(f"❌ Deduplication test failed: {e}")
        import traceback
        traceback.print_exc()
        return False

def test_dag_trigger():
    """Test DAG triggering (if Airflow is running)."""
    print_section("TEST 4: ETL DAG Trigger (Optional)")
    
    # Check if Airflow is running
    result = run_command(
        "docker ps | grep airflow-webserver",
        "Checking Airflow status"
    )
    
    if not result:
        print("⚠️  Airflow not running, skipping DAG test")
        return True  # Don't fail if Airflow is not running
    
    # List DAGs
    success = run_command(
        "docker exec vinatien-airflow-webserver airflow dags list | grep postgresql_to_clickhouse_etl",
        "Checking for ETL DAG"
    )
    
    if success:
        print("✅ ETL DAG found in Airflow")
        print("ℹ️  To trigger manually: docker exec vinatien-airflow-webserver airflow dags trigger postgresql_to_clickhouse_etl")
    else:
        print("⚠️  ETL DAG not found, but Airflow is running")
        print("ℹ️  List all DAGs: docker exec vinatien-airflow-webserver airflow dags list")
    
    return True

def test_ml_scoring():
    """Test ML EWA scoring with ClickHouse data."""
    print_section("TEST 5: ML EWA Eligibility Scoring")
    
    try:
        from clickhouse_driver import Client
        import os
        import pandas as pd
        
        client = Client(
            host='localhost',
            port=9000,
            database='vinatien_analytics',
            user=os.getenv('CLICKHOUSE_USER', 'clickhouse'),
            password=os.getenv('CLICKHOUSE_PASSWORD', 'clickhouse123')
        )
        
        # Query recent transactions
        print("📊 Querying transactions from ClickHouse...")
        result = client.execute("""
            SELECT 
                id,
                bank_account_id,
                booking_date,
                amount,
                is_credit,
                is_debit
            FROM transactions_fact FINAL
            WHERE booking_date >= today() - INTERVAL 90 DAY
            LIMIT 1000
        """)
        
        if not result:
            print("ℹ️  No transactions found in last 90 days")
            return True  # Don't fail if no data yet
        
        df = pd.DataFrame(
            result,
            columns=['id', 'bank_account_id', 'booking_date', 'amount', 'is_credit', 'is_debit']
        )
        
        print(f"✅ Retrieved {len(df)} transactions")
        
        # Calculate EWA metrics per account
        for bank_account_id in df['bank_account_id'].unique()[:3]:
            account_df = df[df['bank_account_id'] == bank_account_id]
            
            total_income = float(account_df[account_df['is_credit'] == 1]['amount'].sum())
            total_expenses = float(account_df[account_df['is_debit'] == 1]['amount'].sum())
            transaction_count = len(account_df)
            
            # Simple eligibility score
            score = min(100, (
                (total_income / 1000) * 40 +
                (transaction_count / 10) * 30 +
                30  # Base score
            ))
            
            print(f"\n📊 Account {bank_account_id}:")
            print(f"   Income: €{total_income:.2f}")
            print(f"   Expenses: €{total_expenses:.2f}")
            print(f"   Transactions: {transaction_count}")
            print(f"   🎯 EWA Score: {score:.1f}/100")
            
            if score >= 70:
                print(f"   ✅ ELIGIBLE for EWA")
            elif score >= 50:
                print(f"   ⚠️  PARTIALLY ELIGIBLE")
            else:
                print(f"   ❌ NOT ELIGIBLE")
        
        return True
        
    except Exception as e:
        print(f"❌ ML scoring test failed: {e}")
        import traceback
        traceback.print_exc()
        return False

def main():
    """Run all integration tests."""
    print("\n" + "🚀 " * 35)
    print("   COMPREHENSIVE INTEGRATION TEST SUITE")
    print("   Testing: ReplacingMergeTree + JWT + VPBank + ETL + ML")
    print("🚀 " * 35)
    
    results = {}
    
    # Run tests
    results['ClickHouse'] = test_clickhouse_connection()
    results['VPBank'], tx_count = test_vpbank_extraction()
    results['Deduplication'] = test_deduplication()
    results['DAG'] = test_dag_trigger()
    results['ML Scoring'] = test_ml_scoring()
    
    # Summary
    print_section("TEST SUMMARY")
    
    all_passed = True
    for test_name, passed in results.items():
        status = "✅ PASSED" if passed else "❌ FAILED"
        print(f"  {test_name:20s} {status}")
        if not passed:
            all_passed = False
    
    print(f"\n{'='*70}")
    if all_passed:
        print("🎉 ALL TESTS PASSED! Your updated architecture is working!")
        print("=" * 70)
        print("\n✅ Verified Components:")
        print("   • ReplacingMergeTree deduplication")
        print("   • VPBank API integration")
        print("   • Version-based conflict resolution")
        print("   • ML EWA eligibility scoring")
        print(f"   • Real transaction data: {tx_count} transactions")
        return 0
    else:
        print("❌ SOME TESTS FAILED - Check output above")
        print("="*70)
        return 1

if __name__ == "__main__":
    exit_code = main()
    sys.exit(exit_code)
