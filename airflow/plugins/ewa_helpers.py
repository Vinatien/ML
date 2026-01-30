"""
Airflow Helper Functions for EWA Heuristics Integration

Shared utility functions used across multiple DAGs for:
- User data preparation
- Feature engineering
- Behavioral metrics calculation
- Payday pattern detection

Import in DAGs with:
    from plugins.ewa_helpers import *
"""
from datetime import datetime, timedelta
from typing import List, Dict, Optional, Tuple
import pandas as pd
import numpy as np


def calculate_user_behavioral_metrics(
    transactions_df: pd.DataFrame,
    lookback_days: int = 90
) -> Dict:
    """
    Calculate behavioral risk metrics from transaction history.
    
    Args:
        transactions_df: DataFrame with columns: booking_date, amount, status
        lookback_days: Number of days to look back (default 90)
    
    Returns:
        Dict with metrics: nsf_count, income_volatility, avg_balance, etc.
    """
    
    # Filter to lookback period
    cutoff_date = datetime.now() - timedelta(days=lookback_days)
    recent_tx = transactions_df[
        pd.to_datetime(transactions_df['booking_date']) >= cutoff_date
    ].copy()
    
    # Separate credits and debits
    credits = recent_tx[recent_tx['amount'] > 0]
    debits = recent_tx[recent_tx['amount'] < 0]
    
    # NSF count (transactions with negative balance result)
    # NOTE: This requires balance calculation - simplified for now
    nsf_count = 0  # TODO: Implement NSF detection
    
    # Income volatility (CV of monthly credits)
    if len(credits) > 0:
        monthly_credits = credits.groupby(
            pd.to_datetime(credits['booking_date']).dt.to_period('M')
        )['amount'].sum()
        
        if len(monthly_credits) > 1 and monthly_credits.mean() > 0:
            income_volatility = monthly_credits.std() / monthly_credits.mean()
        else:
            income_volatility = 0.0
    else:
        income_volatility = 0.0
    
    # Average daily balance (simplified - sum of all transactions)
    avg_balance = recent_tx['amount'].sum() / lookback_days if len(recent_tx) > 0 else 0.0
    
    # Transaction frequency
    tx_per_week = len(recent_tx) / (lookback_days / 7.0) if lookback_days > 0 else 0.0
    
    return {
        'nsf_count_90d': nsf_count,
        'income_volatility': float(income_volatility),
        'avg_balance': float(avg_balance),
        'tx_count_90d': len(recent_tx),
        'credit_count_90d': len(credits),
        'debit_count_90d': len(debits),
        'tx_per_week': float(tx_per_week)
    }


def detect_direct_deposit(transactions_df: pd.DataFrame) -> Tuple[bool, Optional[str]]:
    """
    Detect if user has direct deposit (payroll) pattern.
    
    Heuristics:
    - Regular credits > $500
    - From same sender
    - Occurs 2+ times in last 90 days
    
    Args:
        transactions_df: DataFrame with columns: amount, creditor_name, booking_date
    
    Returns:
        (has_direct_deposit: bool, employer_name: Optional[str])
    """
    
    # Filter to credits only (last 90 days)
    cutoff_date = datetime.now() - timedelta(days=90)
    credits = transactions_df[
        (transactions_df['amount'] > 0) &
        (pd.to_datetime(transactions_df['booking_date']) >= cutoff_date)
    ].copy()
    
    if len(credits) < 2:
        return False, None
    
    # Find regular large credits
    large_credits = credits[credits['amount'] > 500]
    
    if len(large_credits) < 2:
        return False, None
    
    # Check for recurring sender
    sender_counts = large_credits['creditor_name'].value_counts()
    
    if len(sender_counts) > 0 and sender_counts.iloc[0] >= 2:
        employer_name = sender_counts.index[0]
        return True, str(employer_name)
    
    return False, None


def prepare_inflow_transactions(transactions_df: pd.DataFrame) -> List[Dict]:
    """
    Prepare transaction list for income inference (Tree B).
    
    Converts DataFrame to format expected by IncomePaydayTree.
    
    Args:
        transactions_df: DataFrame with columns: amount, booking_date, creditor_name
    
    Returns:
        List of dicts with: amount, date, sender_name, description
    """
    
    # Filter to credits (inflows) only
    inflows = transactions_df[transactions_df['amount'] > 0].copy()
    
    # Sort by date descending (most recent first)
    inflows = inflows.sort_values('booking_date', ascending=False)
    
    # Convert to list of dicts
    inflow_list = []
    for _, tx in inflows.iterrows():
        inflow_list.append({
            'amount': float(tx['amount']),
            'date': pd.to_datetime(tx['booking_date']).isoformat(),
            'sender_name': str(tx.get('creditor_name', '')) if pd.notna(tx.get('creditor_name')) else '',
            'description': str(tx.get('description', '')) if pd.notna(tx.get('description')) else ''
        })
    
    return inflow_list


def estimate_accrued_wage(
    last_payday: Optional[datetime],
    hourly_wage: Optional[float],
    hours_per_day: float = 8.0
) -> float:
    """
    Estimate wages accrued since last payday.
    
    Simple calculation:
    days_worked * hours_per_day * hourly_wage
    
    Args:
        last_payday: Date of last payday (or None to assume 14 days ago)
        hourly_wage: Hourly wage rate (or None to use median $20/hr)
        hours_per_day: Working hours per day (default 8)
    
    Returns:
        Estimated accrued wage
    """
    
    # Defaults
    if last_payday is None:
        # Assume biweekly, last payday was 10 days ago (mid-cycle)
        days_worked = 10
    else:
        days_worked = (datetime.now() - last_payday).days
        days_worked = max(0, min(days_worked, 14))  # Cap at 14 days (biweekly)
    
    if hourly_wage is None:
        hourly_wage = 20.0  # Median wage for EWA target demographic
    
    # Assume 5-day work week (exclude weekends)
    work_days = days_worked * (5/7)
    
    accrued = work_days * hours_per_day * hourly_wage
    
    return float(accrued)


def fetch_ewa_history(user_id: int, db_execute_query) -> Dict:
    """
    Fetch EWA advance history for a user.
    
    NOTE: This assumes you've created an `ewa_advances` table.
    Adjust schema based on your actual table structure.
    
    Args:
        user_id: User ID
        db_execute_query: Function to execute SQL queries
    
    Returns:
        Dict with: has_failed_repayment, rapid_redraw, last_advance_date, etc.
    """
    
    # TODO: Replace with actual query when ewa_advances table exists
    # For now, return empty history
    
    # Example query structure:
    """
    query = f'''
    SELECT 
        MAX(CASE WHEN status = 'failed' THEN 1 ELSE 0 END) as has_failed,
        MAX(advance_date) as last_advance_date,
        MAX(repayment_date) as last_repayment_date,
        SUM(CASE 
            WHEN advance_date >= CURRENT_DATE - INTERVAL '7 days' 
            THEN 1 ELSE 0 
        END) as count_last_7d
    FROM ewa_advances
    WHERE user_id = {user_id}
    '''
    
    result = db_execute_query(query)
    
    if len(result) > 0:
        row = result.iloc[0]
        
        # Check for rapid redraw (advanced again within 7 days of repayment)
        rapid_redraw = False
        if pd.notna(row['last_repayment_date']) and pd.notna(row['last_advance_date']):
            days_between = (row['last_advance_date'] - row['last_repayment_date']).days
            rapid_redraw = days_between <= 7
        
        return {
            'has_failed_repayment': bool(row['has_failed']),
            'rapid_redraw': rapid_redraw,
            'last_advance_date': row['last_advance_date'],
            'last_repayment_date': row['last_repayment_date'],
            'recent_advance_count_7d': int(row['count_last_7d'])
        }
    """
    
    # Default empty history
    return {
        'has_failed_repayment': False,
        'rapid_redraw': False,
        'last_advance_date': None,
        'last_repayment_date': None,
        'recent_advance_count_7d': 0
    }


def format_decision_for_clickhouse(decision, user_id: int, account_id: int, batch_id: str) -> Dict:
    """
    Convert EWADecision object to dict ready for ClickHouse insertion.
    
    Args:
        decision: EWADecision object from heuristics engine
        user_id: User ID
        account_id: Account ID
        batch_id: ETL batch ID
    
    Returns:
        Dict matching ewa_eligibility_scores schema
    """
    
    return {
        'scoring_date': datetime.now().date(),
        'user_id': user_id,
        'account_id': account_id,
        
        # Decision
        'approved': 1 if decision.approved else 0,
        'max_advance_amount': decision.max_advance_amount,
        'risk_tier': decision.risk_assessment.tier.value,
        'risk_score': decision.risk_assessment.score,
        
        # Tree A
        'basic_eligibility_passed': 1 if decision.eligibility.eligible else 0,
        
        # Tree B
        'has_payroll_pattern': 1 if decision.income_inference.has_payroll else 0,
        'estimated_monthly_income': decision.income_inference.estimated_monthly_income,
        'payday_pattern': decision.income_inference.payday_pattern,
        'days_to_payday': decision.risk_assessment.days_to_payday,
        
        # Tree C
        'behavioral_flags': decision.risk_assessment.behavioral_flags,
        'pattern_type': decision.risk_assessment.pattern_type or 'unknown',
        'appears_legitimate_use': 1 if decision.risk_assessment.appears_legitimate_use else 0,
        
        # Tree D
        'accrued_wage': decision.advance_limit.accrued_wage if decision.advance_limit else 0.0,
        'percentage_allowed': decision.advance_limit.percentage_allowed if decision.advance_limit else 0.0,
        'fee_amount': decision.fee_amount,
        
        # Tree E
        'cooldown_allowed': 1 if decision.cooldown.allowed else 0,
        'hours_until_next_advance': decision.cooldown.hours_remaining,
        
        # Metadata
        'decision_path': decision.decision_path,
        'rejection_reason': decision.rejection_reason,
        'scoring_timestamp': decision.timestamp,
        'etl_batch_id': batch_id
    }


def calculate_account_age_months(created_at: datetime) -> float:
    """
    Calculate account age in months.
    
    Args:
        created_at: Account creation date
    
    Returns:
        Age in months (fractional)
    """
    days = (datetime.now() - created_at).days
    return days / 30.0


def get_user_employment_months(user_id: int, db_execute_query) -> int:
    """
    Get employment duration from user profile.
    
    TODO: Replace with actual query when employment table exists.
    
    Args:
        user_id: User ID
        db_execute_query: Function to execute SQL queries
    
    Returns:
        Employment duration in months
    """
    
    # TODO: Implement actual query
    # For now, return conservative default
    return 6


def get_user_age(user_id: int, db_execute_query) -> int:
    """
    Get user age from profile.
    
    TODO: Replace with actual query when user profile has DOB.
    
    Args:
        user_id: User ID
        db_execute_query: Function to execute SQL queries
    
    Returns:
        User age in years
    """
    
    # TODO: Implement actual query
    # For now, return safe default
    return 25


def batch_score_users(
    user_data_list: List[Dict],
    heuristic_engine,
    progress_callback=None
) -> Tuple[List[Dict], List[Dict]]:
    """
    Score multiple users with heuristics engine.
    
    Args:
        user_data_list: List of user data dicts (prepared for engine)
        heuristic_engine: EWAHeuristicEngine instance
        progress_callback: Optional function to call with progress updates
    
    Returns:
        (results: List[Dict], errors: List[Dict])
    """
    
    results = []
    errors = []
    
    total = len(user_data_list)
    
    for idx, user_data in enumerate(user_data_list):
        try:
            # Convert date strings to datetime if needed
            if isinstance(user_data.get('last_advance_date'), str):
                user_data['last_advance_date'] = datetime.fromisoformat(user_data['last_advance_date'])
            if isinstance(user_data.get('last_repayment_date'), str):
                user_data['last_repayment_date'] = datetime.fromisoformat(user_data['last_repayment_date'])
            
            # Run heuristics
            decision = heuristic_engine.evaluate(**user_data)
            
            results.append({
                'user_id': decision.user_id,
                'decision': decision
            })
            
        except Exception as e:
            errors.append({
                'user_id': user_data.get('user_id'),
                'error': str(e)
            })
        
        # Progress callback
        if progress_callback and (idx + 1) % 100 == 0:
            progress_callback(idx + 1, total)
    
    return results, errors


# Export all functions
__all__ = [
    'calculate_user_behavioral_metrics',
    'detect_direct_deposit',
    'prepare_inflow_transactions',
    'estimate_accrued_wage',
    'fetch_ewa_history',
    'format_decision_for_clickhouse',
    'calculate_account_age_months',
    'get_user_employment_months',
    'get_user_age',
    'batch_score_users'
]
