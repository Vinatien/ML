"""
Airflow DAG: EWA Batch Heuristic Scoring

This DAG runs the EWA heuristics engine on all active users daily to:
1. Pre-compute eligibility scores for marketing/pre-qualification
2. Monitor portfolio risk (tier distribution, approval rates)
3. Detect behavioral patterns at scale
4. Store results in ClickHouse for analytics

Schedule: Daily at 2:00 AM (after transaction ETL completes)
Author: VinaTien ML Team

Dependencies:
- postgresql_to_clickhouse_etl DAG (must run first)
- ML API heuristics engine
- ClickHouse analytics database
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from pathlib import Path
import sys

# Add project paths for imports
project_root = Path('/opt/airflow')
sys.path.insert(0, str(project_root))

# Default arguments
default_args = {
    'owner': 'vinatien_ml',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'execution_timeout': timedelta(hours=2),  # Longer timeout for batch processing
}

# Create DAG
dag = DAG(
    'ewa_batch_scoring',
    default_args=default_args,
    description='Daily batch EWA heuristic scoring for all active users',
    schedule_interval='0 2 * * *',  # 2:00 AM daily
    start_date=days_ago(1),
    catchup=False,
    tags=['ewa', 'batch', 'heuristics', 'analytics'],
)


def identify_active_users(**context):
    """
    Identify all users eligible for batch scoring.
    
    Criteria:
    - Has bank account connected
    - Had transaction activity in last 90 days
    - Account not closed/suspended
    """
    from config.database import execute_query
    import pandas as pd
    
    print("🔍 Identifying active users for batch scoring...")
    
    query = """
    SELECT DISTINCT
        a.id as user_id,
        ba.id as account_id,
        a.created_at as user_created_at,
        ba.created_at as account_created_at,
        ba.bank_provider,
        ba.consent_status,
        COUNT(t.id) as transaction_count_90d,
        MAX(t.booking_date) as last_transaction_date,
        MIN(t.booking_date) as first_transaction_date
    FROM accounts a
    INNER JOIN bank_accounts ba ON ba.account_id = a.id
    LEFT JOIN transactions t ON t.bank_account_id = ba.id
        AND t.booking_date >= CURRENT_DATE - INTERVAL '90 days'
    WHERE ba.is_active = true
        AND ba.consent_status IN ('valid', 'active')
    GROUP BY a.id, ba.id, a.created_at, ba.created_at, ba.bank_provider, ba.consent_status
    HAVING COUNT(t.id) > 0
    ORDER BY a.id
    """
    
    df = execute_query(query)
    
    if len(df) == 0:
        print("⚠️  No active users found for scoring")
        return {"user_count": 0}
    
    print(f"✅ Found {len(df)} active users for batch scoring")
    print(f"   Date Range: {df['first_transaction_date'].min()} to {df['last_transaction_date'].max()}")
    
    # Save to temp file
    temp_path = Path("/tmp/ewa_active_users.parquet")
    df.to_parquet(temp_path, index=False)
    
    context['ti'].xcom_push(key='user_count', value=len(df))
    
    return {"user_count": len(df)}


def prepare_user_data(**context):
    """
    For each active user, gather all data needed by heuristics engine:
    - Account age, user age, employment info
    - Transaction history (last 90 days)
    - EWA history (if any)
    - Behavioral signals (NSF, volatility, etc.)
    """
    import pandas as pd
    import numpy as np
    from config.database import execute_query
    from datetime import datetime, timedelta
    import pytz
    
    print("📊 Preparing user data for batch scoring...")
    
    # Read active users
    temp_path = Path("/tmp/ewa_active_users.parquet")
    users_df = pd.read_parquet(temp_path)
    
    all_user_data = []
    
    for idx, user_row in users_df.iterrows():
        user_id = user_row['user_id']
        account_id = user_row['account_id']
        
        # Calculate account age (make datetime timezone-aware)
        account_created = pd.to_datetime(user_row['account_created_at'])
        now = datetime.now(pytz.UTC)
        account_age_months = (now - account_created).days / 30.0
        
        # Fetch transactions for this user (last 90 days)
        tx_query = f"""
        SELECT 
            booking_date,
            amount,
            creditor_name,
            debtor_name,
            booking_status
        FROM transactions
        WHERE bank_account_id = {account_id}
          AND booking_date >= CURRENT_DATE - INTERVAL '90 days'
          AND booking_status = 'booked'
        ORDER BY booking_date DESC
        """
        
        transactions_df = execute_query(tx_query)
        
        # Separate inflows (credits) and outflows (debits)
        inflows = transactions_df[transactions_df['amount'] > 0].copy()
        outflows = transactions_df[transactions_df['amount'] < 0].copy()
        
        # Calculate behavioral metrics
        nsf_count = 0  # TODO: Implement NSF detection logic
        
        # Income volatility (coefficient of variation of monthly credits)
        if len(inflows) > 0:
            monthly_credits = inflows.groupby(
                pd.to_datetime(inflows['booking_date']).dt.to_period('M')
            )['amount'].sum()
            
            if len(monthly_credits) > 1:
                income_volatility = monthly_credits.std() / monthly_credits.mean()
            else:
                income_volatility = 0.0
        else:
            income_volatility = 0.0
        
        # Detect direct deposit (payroll-like transactions)
        has_direct_deposit = False
        if len(inflows) >= 2:
            # Simple heuristic: regular credits > $500 from same sender
            regular_senders = inflows[inflows['amount'] > 500]['creditor_name'].value_counts()
            if len(regular_senders) > 0 and regular_senders.iloc[0] >= 2:
                has_direct_deposit = True
        
        # Prepare inflow transactions for income inference (Tree B)
        inflow_transactions = []
        for _, tx in inflows.iterrows():
            inflow_transactions.append({
                'amount': float(tx['amount']),
                'date': pd.to_datetime(tx['booking_date']).isoformat(),
                'sender_name': str(tx['creditor_name']) if pd.notna(tx['creditor_name']) else '',
                'description': ''  # Add if available in your schema
            })
        
        # Fetch EWA history (if table exists)
        # TODO: Replace with actual query when ewa_advances table is created
        ewa_history = {
            'has_failed_repayment': False,
            'rapid_redraw': False,
            'last_advance_date': None,
            'last_repayment_date': None,
            'recent_advance_count_7d': 0
        }
        
        # Assemble user data payload
        user_data = {
            'user_id': int(user_id),
            'account_id': int(account_id),
            
            # Tree A inputs
            'user_age': 25,  # TODO: Get from user profile table
            'account_age_months': int(account_age_months),  # Convert to int
            'has_direct_deposit': bool(has_direct_deposit),
            'employment_months': 6,  # TODO: Get from employment verification
            
            # Tree B inputs
            'inflow_transactions': inflow_transactions,
            
            # Tree C inputs
            'nsf_count_90d': int(nsf_count),
            'income_volatility': float(income_volatility),
            'prior_ewa_failed': bool(ewa_history['has_failed_repayment']),
            'rapid_redraw': bool(ewa_history['rapid_redraw']),
            
            # Tree D inputs
            'accrued_wage': 1500.0,  # TODO: Calculate from hours worked or estimate
            'hourly_wage': 25.0,  # TODO: Get from user profile
            'hours_worked': 80.0,  # TODO: Get from timekeeping integration
            
            # Tree E inputs
            'last_advance_date': ewa_history['last_advance_date'],
            'last_repayment_date': ewa_history['last_repayment_date'],
            'recent_advance_count_7d': int(ewa_history['recent_advance_count_7d']),
            
            # Optional
            'requested_amount': None
        }
        
        all_user_data.append(user_data)
        
        if (idx + 1) % 100 == 0:
            print(f"   Processed {idx + 1}/{len(users_df)} users...")
    
    print(f"✅ Prepared data for {len(all_user_data)} users")
    
    # Save to temp file
    import json
    temp_data_path = Path("/tmp/ewa_user_data_batch.json")
    with open(temp_data_path, 'w') as f:
        json.dump(all_user_data, f)
    
    context['ti'].xcom_push(key='prepared_count', value=len(all_user_data))
    
    return {"prepared_count": len(all_user_data)}


def run_batch_heuristics(**context):
    """
    Run heuristics engine on all prepared users via ML API.
    
    Calls the ML API endpoint for each user to get heuristic scoring.
    The ML API is running at http://vinatien-ml-api:8001
    """
    import json
    import pandas as pd
    from datetime import datetime
    import requests
    
    print("🤖 Running batch heuristic scoring via ML API...")
    
    # Load prepared user data
    temp_data_path = Path("/tmp/ewa_user_data_batch.json")
    with open(temp_data_path, 'r') as f:
        user_data_list = json.load(f)
    
    # ML API endpoint (accessible within Docker network)
    ml_api_url = "http://vinatien-ml-api:8001/api/v1/ewa/eligibility"
    
    results = []
    errors = []
    
    print(f"   Processing {len(user_data_list)} users...")
    
    for idx, user_data in enumerate(user_data_list):
        try:
            # Call ML API
            response = requests.post(ml_api_url, json=user_data, timeout=30)
            
            # Check for errors and print detailed response
            if response.status_code != 200:
                error_detail = response.text
                print(f"   ⚠️  Error scoring user_id={user_data['user_id']}: {response.status_code} - {error_detail[:200]}")
                errors.append({'user_id': user_data['user_id'], 'error': f"{response.status_code}: {error_detail[:100]}"})
                continue
            
            response.raise_for_status()
            
            decision = response.json()
            
            # Convert to dict for storage (API returns JSON with nested objects)
            result = {
                'scoring_date': datetime.now().date(),
                'user_id': decision.get('user_id'),
                'account_id': user_data['account_id'],
                
                # Decision
                'approved': 1 if decision.get('approved') else 0,
                'max_advance_amount': decision.get('max_advance_amount', 0.0),
                'risk_tier': decision.get('risk_assessment', {}).get('tier', 'UNKNOWN'),
                'risk_score': decision.get('risk_assessment', {}).get('score', 0),
                
                # Tree outputs
                'basic_eligibility_passed': 1 if decision.get('eligibility', {}).get('eligible') else 0,
                'has_payroll_pattern': 1 if decision.get('income_inference', {}).get('has_payroll') else 0,
                'estimated_monthly_income': decision.get('income_inference', {}).get('estimated_monthly_income', 0.0),
                'payday_pattern': decision.get('income_inference', {}).get('payday_pattern', 'unknown'),
                'days_to_payday': decision.get('risk_assessment', {}).get('days_to_payday', 0),
                
                # Behavioral flags
                'behavioral_flags': str(decision.get('risk_assessment', {}).get('behavioral_flags', [])),
                'pattern_type': decision.get('risk_assessment', {}).get('pattern_type') or 'unknown',
                'appears_legitimate_use': 1 if decision.get('risk_assessment', {}).get('appears_legitimate_use') else 0,
                
                # Limits
                'accrued_wage': decision.get('advance_limit', {}).get('accrued_wage', 0.0) if decision.get('advance_limit') else 0.0,
                'percentage_allowed': decision.get('advance_limit', {}).get('percentage_allowed', 0.0) if decision.get('advance_limit') else 0.0,
                'fee_amount': decision.get('fee_amount', 0.0),
                
                # Cooldown
                'cooldown_allowed': 1 if decision.get('cooldown', {}).get('allowed') else 0,
                'hours_until_next_advance': decision.get('cooldown', {}).get('hours_remaining', 0),
                
                # Metadata
                'decision_path': decision.get('decision_path', ''),
                'rejection_reason': decision.get('rejection_reason', ''),
                'scoring_timestamp': decision.get('timestamp', datetime.now().isoformat()),
                'etl_batch_id': context['ti'].xcom_pull(task_ids='identify_active_users', key='batch_id') or f"batch_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
            }
            
            results.append(result)
            
        except Exception as e:
            print(f"   ⚠️  Error scoring user_id={user_data['user_id']}: {str(e)}")
            errors.append({'user_id': user_data['user_id'], 'error': str(e)})
            continue
        
        if (idx + 1) % 100 == 0:
            print(f"   Scored {idx + 1}/{len(user_data_list)} users...")
    
    print(f"✅ Batch scoring complete:")
    print(f"   Successful: {len(results)}")
    print(f"   Errors: {len(errors)}")
    
    # Save results to temp file
    results_df = pd.DataFrame(results)
    temp_results_path = Path("/tmp/ewa_batch_scores.parquet")
    results_df.to_parquet(temp_results_path, index=False)
    
    # Log error summary
    if errors:
        print("\n❌ Errors encountered:")
        for error in errors[:10]:  # Show first 10
            print(f"   user_id={error['user_id']}: {error['error']}")
    
    context['ti'].xcom_push(key='scored_count', value=len(results))
    context['ti'].xcom_push(key='error_count', value=len(errors))
    
    return {"scored_count": len(results), "error_count": len(errors)}


def save_to_clickhouse(**context):
    """
    Load batch scoring results to ClickHouse analytics database.
    """
    import pandas as pd
    from config.clickhouse import insert_dataframe, get_table_count
    
    print("🗄️  Loading batch scores to ClickHouse...")
    
    # Read results
    temp_results_path = Path("/tmp/ewa_batch_scores.parquet")
    results_df = pd.read_parquet(temp_results_path)
    
    if len(results_df) == 0:
        print("⚠️  No results to load")
        return {"rows_inserted": 0}
    
    # Convert datetime columns
    results_df['scoring_date'] = pd.to_datetime(results_df['scoring_date'])
    results_df['scoring_timestamp'] = pd.to_datetime(results_df['scoring_timestamp'])
    
    # Insert to ClickHouse
    rows_inserted = insert_dataframe(
        results_df,
        table='ewa_eligibility_scores',
        database='vinatien_analytics'
    )
    
    print(f"✅ Loaded {rows_inserted} rows to ClickHouse")
    
    # Get total count
    total_count = get_table_count('vinatien_analytics.ewa_eligibility_scores')
    print(f"   Total rows in table: {total_count:,}")
    
    # Summary stats
    print("\n📊 Batch Scoring Summary:")
    print(f"   Approval Rate: {results_df['approved'].mean()*100:.1f}%")
    print(f"   Avg Max Advance: ${results_df['max_advance_amount'].mean():.2f}")
    print("\n   Risk Tier Distribution:")
    tier_dist = results_df['risk_tier'].value_counts().sort_index()
    for tier, count in tier_dist.items():
        print(f"      Tier {tier}: {count} ({count/len(results_df)*100:.1f}%)")
    
    context['ti'].xcom_push(key='clickhouse_inserted', value=rows_inserted)
    
    return {"rows_inserted": rows_inserted, "total_count": total_count}


def generate_report(**context):
    """
    Generate batch scoring report with key metrics.
    """
    from datetime import datetime
    
    print("✅ BATCH SCORING REPORT")
    print("=" * 70)
    
    # Get metrics from previous tasks
    ti = context['ti']
    
    user_count = ti.xcom_pull(task_ids='identify_active_users', key='user_count') or 0
    prepared_count = ti.xcom_pull(task_ids='prepare_user_data', key='prepared_count') or 0
    scored_count = ti.xcom_pull(task_ids='run_batch_heuristics', key='scored_count') or 0
    error_count = ti.xcom_pull(task_ids='run_batch_heuristics', key='error_count') or 0
    clickhouse_inserted = ti.xcom_pull(task_ids='save_to_clickhouse', key='clickhouse_inserted') or 0
    
    report = f"""
📊 EWA Batch Heuristic Scoring Report
{'=' * 70}

Execution Time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}
Scoring Date: {datetime.now().date()}

Pipeline Metrics:
├─ Active Users Identified: {user_count:,}
├─ User Data Prepared: {prepared_count:,}
├─ Successfully Scored: {scored_count:,}
├─ Errors: {error_count:,}
└─ Loaded to ClickHouse: {clickhouse_inserted:,}

Success Rate: {(scored_count/user_count*100) if user_count > 0 else 0:.1f}%

Next Steps:
1. Query results: SELECT * FROM vinatien_analytics.ewa_eligibility_scores WHERE scoring_date = today()
2. View dashboards: Metabase/Grafana for approval rate trends
3. Export pre-qualified users for marketing campaigns

Status: ✅ SUCCESS
{'=' * 70}
    """
    
    print(report)
    
    # Save report to logs
    log_dir = Path('/opt/airflow/logs')
    log_dir.mkdir(parents=True, exist_ok=True)
    log_file = log_dir / f"ewa_batch_scoring_{datetime.now().strftime('%Y%m%d_%H%M%S')}.txt"
    
    with open(log_file, 'w') as f:
        f.write(report)
    
    print(f"\n📝 Report saved to: {log_file}")
    
    return {
        "status": "success",
        "scored_count": scored_count,
        "error_count": error_count
    }


def cleanup_temp_files():
    """Clean up temporary files."""
    from pathlib import Path
    
    print("🧹 Cleaning up temporary files...")
    
    temp_files = [
        Path("/tmp/ewa_active_users.parquet"),
        Path("/tmp/ewa_user_data_batch.json"),
        Path("/tmp/ewa_batch_scores.parquet")
    ]
    
    for temp_file in temp_files:
        if temp_file.exists():
            temp_file.unlink()
            print(f"   Deleted: {temp_file.name}")
    
    print("✅ Cleanup complete")


# Define tasks
identify_users = PythonOperator(
    task_id='identify_active_users',
    python_callable=identify_active_users,
    provide_context=True,
    dag=dag,
)

prepare_data = PythonOperator(
    task_id='prepare_user_data',
    python_callable=prepare_user_data,
    provide_context=True,
    dag=dag,
)

run_heuristics = PythonOperator(
    task_id='run_batch_heuristics',
    python_callable=run_batch_heuristics,
    provide_context=True,
    dag=dag,
)

load_ch = PythonOperator(
    task_id='save_to_clickhouse',
    python_callable=save_to_clickhouse,
    provide_context=True,
    dag=dag,
)

report = PythonOperator(
    task_id='generate_report',
    python_callable=generate_report,
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
identify_users >> prepare_data >> run_heuristics >> load_ch >> report >> cleanup

# DAG documentation
dag.doc_md = """
# EWA Batch Heuristic Scoring Pipeline

## Overview
This DAG runs the complete EWA heuristics engine on all active users daily.

## What it does:
1. **Identify Active Users**: Find all users with recent transactions
2. **Prepare Data**: Gather account info, transaction history, behavioral metrics
3. **Run Heuristics**: Execute all decision trees (A→B→C→D→E) for each user
4. **Save to ClickHouse**: Store results for analytics
5. **Generate Report**: Summary of approval rates, tier distribution

## Schedule
- **Frequency**: Daily at 2:00 AM
- **Duration**: ~30-60 min for 10,000 users
- **Dependencies**: Runs after `postgresql_to_clickhouse_etl`

## Use Cases
- Pre-qualification for marketing campaigns
- Portfolio risk monitoring
- Behavioral pattern detection
- Approval rate trend analysis

## Monitoring
- Check Airflow UI for task status
- View reports in `/opt/airflow/logs/ewa_batch_scoring_*.txt`
- Query ClickHouse: `SELECT * FROM vinatien_analytics.ewa_eligibility_scores WHERE scoring_date = today()`
"""
