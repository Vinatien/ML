#!/usr/bin/env python3
"""
Synthetic EWA Test Data Generator
Creates realistic transaction patterns in PostgreSQL for comprehensive EWA eligibility testing.

This script generates transactions with realistic patterns:
- Monthly salary deposits
- Regular expenses (rent, utilities, groceries)
- Variable spending
- Different account types (eligible, partially eligible, not eligible)
"""

import sys
import asyncio
from datetime import datetime, timedelta
from decimal import Decimal
import random

sys.path.append('/Users/nguyenvietkhoi/VinaTien/backend')

from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession
from sqlalchemy.orm import sessionmaker
from models.transaction_model import Transaction
from models.bank_account_model import BankAccount
from models.account_model import Account


# Database connection (using postgres superuser for direct access)
DATABASE_URL = "postgresql+asyncpg://postgres:root@localhost:5432/vinatien_db"


def print_section(title):
    """Print formatted section header."""
    print(f"\n{'='*80}")
    print(f"  {title}")
    print(f"{'='*80}\n")


class TransactionPatternGenerator:
    """Generates realistic transaction patterns for different user profiles."""
    
    @staticmethod
    def generate_salary_deposits(start_date: datetime, months: int = 6, salary: float = 2850.00):
        """Generate monthly salary deposits."""
        transactions = []
        
        for i in range(months):
            deposit_date = start_date - timedelta(days=30 * i)
            
            # Add some randomness to salary (bonuses, variations)
            amount = salary
            if random.random() < 0.2:  # 20% chance of bonus
                amount += random.uniform(200, 500)
            
            transactions.append({
                'booking_date': deposit_date.date(),
                'value_date': deposit_date.date(),
                'amount': round(amount, 2),
                'currency': 'EUR',
                'status': 'booked',
                'creditor_name': 'Employer Corp AG',
                'debtor_name': '',
                'creditor_account': '',  # This account
                'debtor_account': 'CH1234567890',
                'transaction_id_bank': f'SAL-{deposit_date.strftime("%Y%m")}-{random.randint(1000,9999)}',
                'source_bank': 'VPBank',
                'description': 'Monthly Salary Payment'
            })
        
        return transactions
    
    @staticmethod
    def generate_rent_payments(start_date: datetime, months: int = 6, rent: float = 850.00):
        """Generate monthly rent payments."""
        transactions = []
        
        for i in range(months):
            payment_date = start_date - timedelta(days=30 * i) - timedelta(days=3)  # Usually paid early
            
            transactions.append({
                'booking_date': payment_date.date(),
                'value_date': payment_date.date(),
                'amount': rent,
                'currency': 'EUR',
                'status': 'booked',
                'creditor_name': 'Property Management GmbH',
                'debtor_name': '',  # This account
                'creditor_account': 'DE123456789',
                'debtor_account': '',
                'transaction_id_bank': f'RENT-{payment_date.strftime("%Y%m")}-{random.randint(1000,9999)}',
                'source_bank': 'VPBank',
                'description': 'Monthly Rent Payment'
            })
        
        return transactions
    
    @staticmethod
    def generate_utility_bills(start_date: datetime, months: int = 6):
        """Generate utility bill payments."""
        transactions = []
        utilities = [
            ('Electric Company', 120, 30),
            ('Water Supply', 45, 30),
            ('Internet Provider', 55, 30),
            ('Mobile Phone', 35, 30),
        ]
        
        for utility_name, base_amount, interval in utilities:
            for i in range(months):
                payment_date = start_date - timedelta(days=interval * i) - timedelta(days=random.randint(5, 10))
                
                # Add seasonal variation
                amount = base_amount + random.uniform(-10, 10)
                
                transactions.append({
                    'booking_date': payment_date.date(),
                    'value_date': payment_date.date(),
                    'amount': round(amount, 2),
                    'currency': 'EUR',
                    'status': 'booked',
                    'creditor_name': utility_name,
                    'debtor_name': '',
                    'creditor_account': f'DE{random.randint(10000000000, 99999999999)}',
                    'debtor_account': '',
                    'transaction_id_bank': f'UTIL-{payment_date.strftime("%Y%m%d")}-{random.randint(1000,9999)}',
                    'source_bank': 'VPBank',
                    'description': f'{utility_name} Bill Payment'
                })
        
        return transactions
    
    @staticmethod
    def generate_grocery_shopping(start_date: datetime, months: int = 6):
        """Generate weekly grocery shopping transactions."""
        transactions = []
        
        for i in range(months * 4):  # ~4 weeks per month
            shopping_date = start_date - timedelta(days=7 * i)
            
            # Grocery amounts vary
            amount = random.uniform(45, 120)
            
            transactions.append({
                'booking_date': shopping_date.date(),
                'value_date': shopping_date.date(),
                'amount': round(amount, 2),
                'currency': 'EUR',
                'status': 'booked',
                'creditor_name': random.choice(['Supermarket A', 'Grocery Store B', 'Food Market C']),
                'debtor_name': '',
                'creditor_account': f'DE{random.randint(10000000000, 99999999999)}',
                'debtor_account': '',
                'transaction_id_bank': f'GROC-{shopping_date.strftime("%Y%m%d")}-{random.randint(1000,9999)}',
                'source_bank': 'VPBank',
                'description': 'Grocery Shopping'
            })
        
        return transactions
    
    @staticmethod
    def generate_miscellaneous_expenses(start_date: datetime, count: int = 30):
        """Generate random miscellaneous expenses."""
        transactions = []
        
        expense_types = [
            ('Restaurant', 25, 80),
            ('Coffee Shop', 3, 8),
            ('Online Shopping', 15, 150),
            ('Pharmacy', 10, 45),
            ('Transportation', 2, 25),
            ('Entertainment', 15, 60),
            ('Clothing Store', 30, 120),
            ('Gas Station', 40, 70),
        ]
        
        for i in range(count):
            expense_name, min_amount, max_amount = random.choice(expense_types)
            expense_date = start_date - timedelta(days=random.randint(0, 180))
            amount = random.uniform(min_amount, max_amount)
            
            transactions.append({
                'booking_date': expense_date.date(),
                'value_date': expense_date.date(),
                'amount': round(amount, 2),
                'currency': 'EUR',
                'status': 'booked',
                'creditor_name': expense_name,
                'debtor_name': '',
                'creditor_account': f'DE{random.randint(10000000000, 99999999999)}',
                'debtor_account': '',
                'transaction_id_bank': f'MISC-{expense_date.strftime("%Y%m%d")}-{random.randint(1000,9999)}',
                'source_bank': 'VPBank',
                'description': f'{expense_name} Purchase'
            })
        
        return transactions


async def create_test_user_with_transactions(session: AsyncSession, profile: dict):
    """Create a test user with bank account and transactions."""
    
    # Create user account
    user = Account(
        username=profile['email'].split('@')[0],  # Use email prefix as username
        full_name=f"{profile['first_name']} {profile['last_name']}",
        email=profile['email'],
        password_hash='test_hash',  # Not used for testing
        is_active=True
    )
    session.add(user)
    await session.flush()
    
    # Create bank account
    from datetime import datetime, timedelta
    bank_account = BankAccount(
        account_id=user.id,
        iban=profile['iban'],
        bank_provider='VPBank',
        consent_id=f"CONSENT-{profile['iban'][-10:]}",
        consent_valid_until=datetime.now() + timedelta(days=90),
        consent_status='valid',
        is_active=True
    )
    session.add(bank_account)
    await session.flush()
    
    # Generate transactions based on profile
    all_transactions = []
    
    # Income
    if profile['has_salary']:
        all_transactions.extend(
            TransactionPatternGenerator.generate_salary_deposits(
                datetime.now(),
                months=profile['months'],
                salary=profile['salary']
            )
        )
    
    # Fixed expenses
    if profile['has_rent']:
        all_transactions.extend(
            TransactionPatternGenerator.generate_rent_payments(
                datetime.now(),
                months=profile['months'],
                rent=profile['rent']
            )
        )
    
    # Utilities
    if profile['has_utilities']:
        all_transactions.extend(
            TransactionPatternGenerator.generate_utility_bills(
                datetime.now(),
                months=profile['months']
            )
        )
    
    # Groceries
    if profile['has_groceries']:
        all_transactions.extend(
            TransactionPatternGenerator.generate_grocery_shopping(
                datetime.now(),
                months=profile['months']
            )
        )
    
    # Misc expenses
    all_transactions.extend(
        TransactionPatternGenerator.generate_miscellaneous_expenses(
            datetime.now(),
            count=profile['misc_count']
        )
    )
    
    # Create transaction records
    for tx_data in all_transactions:
        transaction = Transaction(
            bank_account_id=bank_account.id,
            **tx_data
        )
        session.add(transaction)
    
    return user, bank_account, len(all_transactions)


async def main():
    """Main data generation process."""
    
    print("\n" + "🌱 " * 40)
    print("   SYNTHETIC EWA TEST DATA GENERATOR")
    print("   Creating Realistic Transaction Patterns in PostgreSQL")
    print("🌱 " * 40)
    
    # Create async engine
    engine = create_async_engine(DATABASE_URL, echo=False)
    async_session = sessionmaker(
        engine, class_=AsyncSession, expire_on_commit=False
    )
    
    # Define test profiles
    test_profiles = [
        {
            'profile_name': 'HIGH EARNER - Eligible for EWA',
            'first_name': 'Alice',
            'last_name': 'Johnson',
            'email': 'alice.johnson@test.com',
            'phone': '+41791234501',
            'address': 'Test Street 1, Zurich',
            'iban': 'LI2108805500000001001',
            'initial_balance': '5420.50',
            'has_salary': True,
            'salary': 4500.00,  # High salary
            'has_rent': True,
            'rent': 1200.00,
            'has_utilities': True,
            'has_groceries': True,
            'misc_count': 40,
            'months': 6,
            'expected_eligibility': 'ELIGIBLE'
        },
        {
            'profile_name': 'AVERAGE EARNER - Partially Eligible',
            'first_name': 'Bob',
            'last_name': 'Smith',
            'email': 'bob.smith@test.com',
            'phone': '+41791234502',
            'address': 'Test Street 2, Geneva',
            'iban': 'LI2108805500000001002',
            'initial_balance': '2150.75',
            'has_salary': True,
            'salary': 2850.00,  # Average salary
            'has_rent': True,
            'rent': 850.00,
            'has_utilities': True,
            'has_groceries': True,
            'misc_count': 25,
            'months': 6,
            'expected_eligibility': 'PARTIALLY ELIGIBLE'
        },
        {
            'profile_name': 'LOW EARNER - Not Eligible',
            'first_name': 'Charlie',
            'last_name': 'Davis',
            'email': 'charlie.davis@test.com',
            'phone': '+41791234503',
            'address': 'Test Street 3, Basel',
            'iban': 'LI2108805500000001003',
            'initial_balance': '420.30',
            'has_salary': True,
            'salary': 1800.00,  # Lower salary
            'has_rent': True,
            'rent': 650.00,
            'has_utilities': False,
            'has_groceries': True,
            'misc_count': 15,
            'months': 6,
            'expected_eligibility': 'NOT ELIGIBLE'
        },
        {
            'profile_name': 'IRREGULAR INCOME - Edge Case',
            'first_name': 'Diana',
            'last_name': 'Martinez',
            'email': 'diana.martinez@test.com',
            'phone': '+41791234504',
            'address': 'Test Street 4, Bern',
            'iban': 'LI2108805500000001004',
            'initial_balance': '890.00',
            'has_salary': False,  # No regular salary
            'salary': 0,
            'has_rent': False,
            'rent': 0,
            'has_utilities': False,
            'has_groceries': False,
            'misc_count': 50,  # Only irregular expenses
            'months': 6,
            'expected_eligibility': 'NOT ELIGIBLE - No Income'
        },
    ]
    
    print_section("GENERATING TEST USERS AND TRANSACTIONS")
    
    total_users = 0
    total_transactions = 0
    
    async with async_session() as session:
        async with session.begin():
            for profile in test_profiles:
                print(f"\n📊 Creating: {profile['profile_name']}")
                print(f"   Name: {profile['first_name']} {profile['last_name']}")
                print(f"   IBAN: {profile['iban']}")
                print(f"   Expected: {profile['expected_eligibility']}")
                
                try:
                    user, bank_account, tx_count = await create_test_user_with_transactions(
                        session,
                        profile
                    )
                    
                    print(f"   ✅ Created user {user.id} with {tx_count} transactions")
                    total_users += 1
                    total_transactions += tx_count
                    
                except Exception as e:
                    print(f"   ❌ Failed: {e}")
                    raise
    
    await engine.dispose()
    
    # Summary
    print_section("GENERATION COMPLETE")
    
    print(f"✅ Successfully created:")
    print(f"   Users: {total_users}")
    print(f"   Total Transactions: {total_transactions}")
    print(f"   Average per user: {total_transactions // total_users if total_users > 0 else 0}")
    
    print("\n" + "="*80)
    print("  TEST PROFILE SUMMARY")
    print("="*80)
    
    for profile in test_profiles:
        print(f"\n{profile['profile_name']}")
        print(f"  IBAN: {profile['iban']}")
        print(f"  Monthly Salary: €{profile['salary']:.2f}")
        print(f"  Monthly Rent: €{profile['rent']:.2f}")
        print(f"  Transaction Period: {profile['months']} months")
        print(f"  Expected EWA Result: {profile['expected_eligibility']}")
    
    print("\n" + "="*80)
    print("  NEXT STEPS")
    print("="*80)
    print("""
1. ✅ Data created in PostgreSQL
   
2. Trigger ETL to load into ClickHouse:
   docker exec vinatien-airflow-webserver airflow dags trigger postgresql_to_clickhouse_etl \\
       --conf '{"start_date": "2024-08-01", "end_date": "2026-02-03"}'

3. Verify data in ClickHouse:
   docker exec ml-clickhouse-1 clickhouse-client \\
       --database vinatien_analytics \\
       --query "SELECT bank_account_id, COUNT(*) as tx_count, SUM(amount) as total FROM transactions_fact FINAL GROUP BY bank_account_id"

4. Run EWA scoring tests:
   cd /Users/nguyenvietkhoi/VinaTien/ML && ./run_tests.sh

5. Test ML scoring on specific accounts:
   python3 test_ewa_scoring.py --iban LI2108805500000001001  # High earner
   python3 test_ewa_scoring.py --iban LI2108805500000001002  # Average earner
   python3 test_ewa_scoring.py --iban LI2108805500000001003  # Low earner
   python3 test_ewa_scoring.py --iban LI2108805500000001004  # Irregular income
    """)
    
    return 0


if __name__ == "__main__":
    try:
        exit_code = asyncio.run(main())
        sys.exit(exit_code)
    except Exception as e:
        print(f"\n❌ FATAL ERROR: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)
