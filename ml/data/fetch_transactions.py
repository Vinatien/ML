"""
Fetch transaction data from the database for ML workflows.
This script retrieves transaction data and prepares it for analysis.
"""

import sys
from pathlib import Path
from typing import Optional
import pandas as pd
from datetime import datetime, timedelta

# Add parent directory to path to import config
sys.path.append(str(Path(__file__).parent.parent))
from config.database import DatabaseConfig, execute_query


def fetch_all_transactions() -> pd.DataFrame:
    """
    Fetch all transactions from the database.
    
    Returns:
        DataFrame containing all transactions
    """
    query = """
    SELECT 
        t.id,
        t.booking_date,
        t.value_date,
        t.amount,
        t.currency,
        t.booking_status,
        t.creditor_name,
        t.debtor_name,
        t.creditor_account_last4,
        t.debtor_account_last4,
        t.bank_account_id,
        t.created_at,
        ba.bank_provider,
        ba.iban as account_iban
    FROM transactions t
    LEFT JOIN bank_accounts ba ON t.bank_account_id = ba.id
    ORDER BY t.booking_date DESC
    """
    
    print("Fetching all transactions...")
    df = execute_query(query)
    print(f"✅ Fetched {len(df)} transactions")
    return df


def fetch_transactions_by_date_range(
    start_date: str, 
    end_date: str
) -> pd.DataFrame:
    """
    Fetch transactions within a specific date range.
    
    Args:
        start_date: Start date in 'YYYY-MM-DD' format
        end_date: End date in 'YYYY-MM-DD' format
        
    Returns:
        DataFrame containing filtered transactions
    """
    query = """
    SELECT 
        t.id,
        t.booking_date,
        t.value_date,
        t.amount,
        t.currency,
        t.booking_status,
        t.creditor_name,
        t.debtor_name,
        t.creditor_account_last4,
        t.debtor_account_last4,
        t.bank_account_id,
        t.created_at
    FROM transactions t
    WHERE t.booking_date BETWEEN %s AND %s
    ORDER BY t.booking_date DESC
    """
    
    print(f"Fetching transactions from {start_date} to {end_date}...")
    df = execute_query(query, (start_date, end_date))
    print(f"✅ Fetched {len(df)} transactions")
    return df


def fetch_recent_transactions(days: int = 30) -> pd.DataFrame:
    """
    Fetch transactions from the last N days.
    
    Args:
        days: Number of days to look back
        
    Returns:
        DataFrame containing recent transactions
    """
    end_date = datetime.now()
    start_date = end_date - timedelta(days=days)
    
    return fetch_transactions_by_date_range(
        start_date.strftime('%Y-%m-%d'),
        end_date.strftime('%Y-%m-%d')
    )


def get_transaction_statistics() -> dict:
    """
    Get summary statistics about transactions.
    
    Returns:
        Dictionary containing various statistics
    """
    queries = {
        'total_transactions': "SELECT COUNT(*) as count FROM transactions",
        'total_amount': """
            SELECT 
                SUM(amount) as total,
                AVG(amount) as average,
                MIN(amount) as minimum,
                MAX(amount) as maximum,
                currency
            FROM transactions
            GROUP BY currency
        """,
        'by_status': """
            SELECT 
                booking_status,
                COUNT(*) as count,
                SUM(amount) as total_amount
            FROM transactions
            GROUP BY booking_status
        """,
        'date_range': """
            SELECT 
                MIN(booking_date) as earliest,
                MAX(booking_date) as latest
            FROM transactions
        """
    }
    
    stats = {}
    for key, query in queries.items():
        stats[key] = execute_query(query)
    
    return stats


def prepare_ml_dataset(
    include_account_info: bool = True,
    filter_status: Optional[str] = None
) -> pd.DataFrame:
    """
    Prepare a clean dataset for ML training.
    
    Args:
        include_account_info: Whether to include bank account information
        filter_status: Optional filter for booking_status ('booked' or 'pending')
        
    Returns:
        DataFrame ready for ML processing
    """
    query = """
    SELECT 
        t.id,
        t.booking_date,
        t.value_date,
        t.amount,
        t.currency,
        t.booking_status,
        t.creditor_name,
        t.debtor_name,
        t.creditor_account_last4,
        t.debtor_account_last4,
        t.bank_account_id
        {account_fields}
    FROM transactions t
    {account_join}
    {status_filter}
    ORDER BY t.booking_date DESC
    """
    
    account_fields = ""
    account_join = ""
    if include_account_info:
        account_fields = ", ba.bank_provider, ba.iban"
        account_join = "LEFT JOIN bank_accounts ba ON t.bank_account_id = ba.id"
    
    status_filter = ""
    params = None
    if filter_status:
        status_filter = "WHERE t.booking_status = %s"
        params = (filter_status,)
    
    query = query.format(
        account_fields=account_fields,
        account_join=account_join,
        status_filter=status_filter
    )
    
    print(f"Preparing ML dataset (filter: {filter_status or 'none'})...")
    df = execute_query(query, params)
    
    # Convert dates to datetime
    df['booking_date'] = pd.to_datetime(df['booking_date'])
    if 'value_date' in df.columns:
        df['value_date'] = pd.to_datetime(df['value_date'])
    
    # Add derived features
    df['amount_abs'] = df['amount'].abs()
    df['is_credit'] = df['amount'] > 0
    df['is_debit'] = df['amount'] < 0
    df['day_of_week'] = df['booking_date'].dt.dayofweek
    df['month'] = df['booking_date'].dt.month
    df['year'] = df['booking_date'].dt.year
    
    print(f"✅ Prepared dataset with {len(df)} records and {len(df.columns)} features")
    return df


if __name__ == "__main__":
    # Example usage
    print("\n" + "="*60)
    print("TRANSACTION DATA FETCHER")
    print("="*60 + "\n")
    
    # Test connection
    from config.database import test_connection
    if not test_connection():
        sys.exit(1)
    
    # Fetch all transactions
    print("\n1. Fetching all transactions...")
    df_all = fetch_all_transactions()
    print(f"Shape: {df_all.shape}")
    print(f"\nFirst 5 rows:")
    print(df_all.head())
    
    # Get statistics
    print("\n2. Getting statistics...")
    stats = get_transaction_statistics()
    print("\nTotal transactions:")
    print(stats['total_transactions'])
    print("\nBy status:")
    print(stats['by_status'])
    print("\nDate range:")
    print(stats['date_range'])
    
    # Prepare ML dataset
    print("\n3. Preparing ML dataset...")
    df_ml = prepare_ml_dataset(filter_status='booked')
    print(f"ML dataset shape: {df_ml.shape}")
    print(f"\nColumns: {df_ml.columns.tolist()}")
