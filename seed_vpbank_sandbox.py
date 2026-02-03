#!/usr/bin/env python3
"""
VPBank Sandbox Data Seeder
Creates realistic transaction data in VPBank sandbox for EWA eligibility testing.

This script will:
1. Create a consent and get an IBAN
2. Attempt to create mock deposit transactions (salary, income)
3. Create PISP payment transactions (expenses)
4. Verify the transactions were created
5. Provide summary for EWA testing
"""

import sys
import time
from datetime import datetime, timedelta
from decimal import Decimal

sys.path.append('/Users/nguyenvietkhoi/VinaTien/backend')

import requests
from app.bank.vpbank import VPBank


def print_section(title):
    """Print formatted section header."""
    print(f"\n{'='*80}")
    print(f"  {title}")
    print(f"{'='*80}\n")


def create_salary_pattern(vpbank: VPBank, account_id: str):
    """
    Create a realistic salary pattern (monthly deposits).
    Attempts to use VPBank sandbox mock deposit endpoint.
    """
    print_section("CREATING SALARY PATTERN (Mock Deposits)")
    
    print("📊 Attempting to create 3 months of salary deposits...")
    print("⚠️  Note: VPBank sandbox may not support mock deposit creation")
    print("   This uses a non-standard /sandbox/accounts/{iban}/transactions endpoint\n")
    
    salaries = [
        {"amount": "2850.00", "date": datetime.now() - timedelta(days=60), "desc": "Monthly Salary - December"},
        {"amount": "2850.00", "date": datetime.now() - timedelta(days=30), "desc": "Monthly Salary - January"},
        {"amount": "2850.00", "date": datetime.now() - timedelta(days=5), "desc": "Monthly Salary - February"},
    ]
    
    success_count = 0
    failed_count = 0
    
    for salary in salaries:
        print(f"💰 Creating deposit: €{salary['amount']} ({salary['desc']})...")
        
        # Try to use the mock deposit method
        result = vpbank.create_mock_deposit(account_id, salary['amount'])
        
        if result:
            success_count += 1
            time.sleep(0.5)  # Small delay between requests
        else:
            failed_count += 1
    
    print(f"\n📊 Results: {success_count} successful, {failed_count} failed")
    
    if failed_count > 0:
        print("\n⚠️  VPBank sandbox doesn't support mock deposit endpoint.")
        print("   Alternative approach: Use PISP payments instead (see below)")
    
    return success_count > 0


def create_expense_pattern(vpbank: VPBank, account_id: str):
    """
    Create realistic expense transactions using PISP (Payment Initiation).
    These will appear as OUTGOING transactions (debits).
    """
    print_section("CREATING EXPENSE PATTERN (PISP Payments)")
    
    print("📊 Creating expense transactions using Payment Initiation...")
    print("ℹ️  These will create PENDING transactions that simulate expenses\n")
    
    expenses = [
        {"amount": "45.50", "creditor": "Grocery Store", "iban": "DE89370400440532013001"},
        {"amount": "120.00", "creditor": "Utility Company", "iban": "DE89370400440532013002"},
        {"amount": "35.75", "creditor": "Restaurant", "iban": "DE89370400440532013003"},
        {"amount": "89.99", "creditor": "Online Shopping", "iban": "DE89370400440532013004"},
        {"amount": "250.00", "creditor": "Rent Payment", "iban": "DE89370400440532013005"},
    ]
    
    payment_ids = []
    
    for expense in expenses:
        print(f"💳 Creating payment: €{expense['amount']} to {expense['creditor']}...")
        
        try:
            payment_id = vpbank.make_payment(
                debtor_iban=account_id,
                amount=expense['amount'],
                creditor_iban=expense['iban'],
                creditor_bic="COBADEFF"
            )
            payment_ids.append(payment_id)
            print(f"   ✅ Payment ID: {payment_id}")
            time.sleep(0.5)  # Small delay between payments
            
        except Exception as e:
            print(f"   ❌ Failed: {e}")
    
    print(f"\n📊 Created {len(payment_ids)} expense transactions")
    return payment_ids


def create_income_pattern_via_payments(vpbank: VPBank, account_id: str):
    """
    Alternative: Create income-like transactions by having another account pay to this one.
    Note: This requires reversing debtor/creditor, which may not work in sandbox.
    """
    print_section("CREATING INCOME PATTERN (Alternative Method)")
    
    print("⚠️  This method attempts to create incoming payments")
    print("   by reversing debtor/creditor roles.")
    print("   VPBank sandbox may not support this.\n")
    
    # This typically won't work in sandbox, but worth documenting
    print("❌ Skipping - VPBank sandbox doesn't support external accounts paying IN")
    print("   Sandbox accounts can only make OUTGOING payments via PISP\n")
    
    return False


def verify_transactions(vpbank: VPBank, account_id: str):
    """Verify all created transactions."""
    print_section("VERIFYING CREATED TRANSACTIONS")
    
    success, tx_data = vpbank.get_transactions_and_review(account_id, "Verification")
    
    if success and tx_data:
        booked = tx_data.get("booked", [])
        pending = tx_data.get("pending", [])
        
        total_credit = sum(
            float(tx.get('transactionAmount', {}).get('amount', 0))
            for tx in (booked + pending)
            if tx.get('debtorAccount', {}).get('iban') != account_id
        )
        
        total_debit = sum(
            float(tx.get('transactionAmount', {}).get('amount', 0))
            for tx in (booked + pending)
            if tx.get('debtorAccount', {}).get('iban') == account_id
        )
        
        return {
            'total_transactions': len(booked) + len(pending),
            'booked': len(booked),
            'pending': len(pending),
            'total_credit': total_credit,
            'total_debit': total_debit,
            'data': tx_data
        }
    
    return None


def main():
    """Main seeding process."""
    print("\n" + "🌱 " * 40)
    print("   VPBANK SANDBOX DATA SEEDER")
    print("   Creating Realistic Transaction Data for EWA Testing")
    print("🌱 " * 40)
    
    # Initialize VPBank session
    session = requests.Session()
    session.headers.update({
        "Content-Type": "application/json",
        "Accept": "application/json",
        "TPP-Redirect-URI": "https://www.google.ch",
        "PSU-IP-Address": "192.0.0.12"
    })
    
    vpbank = VPBank(session)
    
    # Step 1: Get account IBAN
    print_section("STEP 1: Getting Account IBAN")
    try:
        account_id = vpbank.create_consent_and_get_iban()
        print(f"\n✅ Target Account: {account_id}")
    except Exception as e:
        print(f"\n❌ Failed to get account: {e}")
        return 1
    
    # Step 2: Check current state
    print_section("STEP 2: Current Transaction State")
    initial_state = verify_transactions(vpbank, account_id)
    
    if initial_state:
        print(f"\n📊 Current State:")
        print(f"   Total Transactions: {initial_state['total_transactions']}")
        print(f"   Booked: {initial_state['booked']}, Pending: {initial_state['pending']}")
        print(f"   Total Credits: €{initial_state['total_credit']:.2f}")
        print(f"   Total Debits: €{initial_state['total_debit']:.2f}")
    
    # Step 3: Attempt to create salary deposits (likely to fail)
    salary_success = create_salary_pattern(vpbank, account_id)
    
    # Step 4: Create expense transactions (should work)
    payment_ids = create_expense_pattern(vpbank, account_id)
    
    # Step 5: Verify final state
    print("\n⏳ Waiting 3 seconds for transactions to settle...")
    time.sleep(3)
    
    final_state = verify_transactions(vpbank, account_id)
    
    # Summary
    print_section("SUMMARY & RECOMMENDATIONS")
    
    if final_state:
        print(f"✅ Final Transaction State:")
        print(f"   Total Transactions: {final_state['total_transactions']}")
        print(f"   Booked: {final_state['booked']}, Pending: {final_state['pending']}")
        print(f"   Total Credits (Income): €{final_state['total_credit']:.2f}")
        print(f"   Total Debits (Expenses): €{final_state['total_debit']:.2f}")
        
        if final_state['total_transactions'] > 0:
            print(f"\n🎉 SUCCESS! Created {final_state['total_transactions']} transactions")
        else:
            print(f"\n⚠️  No transactions created")
    
    print("\n" + "="*80)
    print("  VPBANK SANDBOX LIMITATIONS")
    print("="*80)
    print("""
The VPBank Berlin Group sandbox has the following limitations:

1. ❌ NO MOCK DEPOSIT ENDPOINT
   - Cannot create incoming credit transactions directly
   - /sandbox/accounts/{iban}/transactions POST returns 404
   
2. ✅ PISP PAYMENTS WORK
   - Can create outgoing payment (debit) transactions
   - These appear as PENDING transactions
   - Simulate expenses/spending
   
3. ❌ LIMITED INCOME SIMULATION
   - Cannot simulate salary deposits in sandbox
   - Cannot create transactions from external accounts
   
4. 📋 WHAT THIS MEANS FOR EWA TESTING:
   - Sandbox can only test EXPENSE patterns
   - Cannot test income-based eligibility
   - Cannot calculate debt-to-income ratios
   - Limited behavioral pattern detection
    """)
    
    print("\n" + "="*80)
    print("  ALTERNATIVE APPROACHES FOR EWA TESTING")
    print("="*80)
    print("""
1. 🎯 USE SYNTHETIC DATA IN POSTGRESQL
   - Create realistic transaction data directly in PostgreSQL
   - Full control over income/expense patterns
   - Test all EWA eligibility scenarios
   - See: create_synthetic_ewa_test_data.py (below)

2. 🔄 USE EXISTING POSTGRESQL DATA
   - You already have 8 transactions in PostgreSQL
   - Run ETL to ingest into ClickHouse
   - Test ML scoring on existing data
   
3. 🌐 PRODUCTION VPBANK API (FUTURE)
   - Real customer accounts have full transaction history
   - True income patterns (salary deposits)
   - Realistic spending behavior
   - This is the ultimate test environment

RECOMMENDED: Create synthetic data in PostgreSQL for comprehensive testing.
    """)
    
    print("\n" + "="*80)
    print("  NEXT STEPS")
    print("="*80)
    print(f"""
1. Run synthetic data generator:
   python3 /Users/nguyenvietkhoi/VinaTien/ML/create_synthetic_ewa_test_data.py

2. Trigger ETL to load into ClickHouse:
   docker exec vinatien-airflow-webserver airflow dags trigger postgresql_to_clickhouse_etl

3. Run ML scoring tests:
   cd /Users/nguyenvietkhoi/VinaTien/ML && ./run_tests.sh

4. Current VPBank Account IBAN: {account_id}
   - Has {final_state['total_transactions'] if final_state else 0} transactions
   - Can be used for API integration testing
    """)
    
    return 0


if __name__ == "__main__":
    try:
        exit_code = main()
        sys.exit(exit_code)
    except Exception as e:
        print(f"\n❌ FATAL ERROR: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)
