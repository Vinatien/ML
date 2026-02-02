#!/usr/bin/env python3
"""
ClickHouse Deduplication Helper Script

This script helps identify and remove duplicate entries from the 
ewa_eligibility_scores table.

Usage:
    # Check for duplicates
    python cleanup_duplicates.py --check

    # Remove duplicates (keeps most recent record)
    python cleanup_duplicates.py --remove

    # Remove duplicates for specific date
    python cleanup_duplicates.py --remove --date 2026-01-31
"""

import argparse
from datetime import datetime, date
import sys
from pathlib import Path

# Add project paths
project_root = Path(__file__).parent.parent.parent
sys.path.insert(0, str(project_root))

from config.clickhouse import execute_clickhouse_query


def check_duplicates(scoring_date=None):
    """
    Check for duplicate entries in ewa_eligibility_scores table.
    
    Args:
        scoring_date: Optional date string (YYYY-MM-DD) to check specific date
    
    Returns:
        List of duplicate entries
    """
    print("🔍 Checking for duplicate entries...")
    
    date_filter = f"WHERE scoring_date = '{scoring_date}'" if scoring_date else ""
    
    query = f"""
    SELECT 
        scoring_date,
        user_id,
        account_id,
        COUNT(*) as duplicate_count,
        groupArray(scoring_timestamp) as timestamps
    FROM vinatien_analytics.ewa_eligibility_scores
    {date_filter}
    GROUP BY scoring_date, user_id, account_id
    HAVING duplicate_count > 1
    ORDER BY duplicate_count DESC, scoring_date DESC
    """
    
    duplicates = execute_clickhouse_query(query)
    
    if not duplicates:
        print("✅ No duplicates found!")
        return []
    
    print(f"\n⚠️  Found {len(duplicates)} sets of duplicate entries:\n")
    
    total_duplicate_rows = 0
    for i, dup in enumerate(duplicates[:20], 1):  # Show first 20
        total_duplicate_rows += dup['duplicate_count'] - 1  # -1 because we keep one
        print(f"{i}. scoring_date={dup['scoring_date']}, "
              f"user_id={dup['user_id']}, "
              f"account_id={dup['account_id']}, "
              f"count={dup['duplicate_count']}")
        
        # Show timestamps to see which is most recent
        if len(dup['timestamps']) <= 5:
            print(f"   Timestamps: {', '.join(str(ts) for ts in dup['timestamps'])}")
    
    if len(duplicates) > 20:
        print(f"\n   ... and {len(duplicates) - 20} more")
    
    print(f"\n📊 Summary:")
    print(f"   Duplicate Groups: {len(duplicates)}")
    print(f"   Extra Rows to Remove: {total_duplicate_rows}")
    
    return duplicates


def remove_duplicates(scoring_date=None, dry_run=False):
    """
    Remove duplicate entries, keeping only the most recent record for each
    (scoring_date, user_id, account_id) combination.
    
    Args:
        scoring_date: Optional date string (YYYY-MM-DD) to clean specific date
        dry_run: If True, only show what would be deleted
    
    Returns:
        Number of rows deleted
    """
    print("🧹 Removing duplicate entries...")
    
    if dry_run:
        print("⚠️  DRY RUN MODE - No actual deletions will be performed\n")
    
    # First, check for duplicates
    duplicates = check_duplicates(scoring_date)
    
    if not duplicates:
        print("✅ No duplicates to remove")
        return 0
    
    if dry_run:
        print("\n⚠️  Dry run complete. Run without --dry-run to actually delete duplicates.")
        return 0
    
    # Confirm deletion
    response = input(f"\n⚠️  This will DELETE duplicate records. Continue? (yes/no): ")
    if response.lower() not in ['yes', 'y']:
        print("❌ Deletion cancelled")
        return 0
    
    print("\n🗑️  Deleting duplicates...")
    
    date_filter = f"AND scoring_date = '{scoring_date}'" if scoring_date else ""
    
    # Strategy: For each duplicate group, keep only the row with the latest scoring_timestamp
    # We'll use a CTE to identify rows to delete
    
    delete_query = f"""
    ALTER TABLE vinatien_analytics.ewa_eligibility_scores
    DELETE WHERE (scoring_date, user_id, account_id, scoring_timestamp) IN (
        SELECT 
            scoring_date,
            user_id,
            account_id,
            scoring_timestamp
        FROM vinatien_analytics.ewa_eligibility_scores
        WHERE (scoring_date, user_id, account_id) IN (
            SELECT scoring_date, user_id, account_id
            FROM vinatien_analytics.ewa_eligibility_scores
            {date_filter}
            GROUP BY scoring_date, user_id, account_id
            HAVING COUNT(*) > 1
        )
        {date_filter}
        QUALIFY row_number() OVER (PARTITION BY scoring_date, user_id, account_id ORDER BY scoring_timestamp DESC) > 1
    )
    """
    
    try:
        result = execute_clickhouse_query(delete_query)
        print("✅ Duplicates deleted successfully!")
        
        # Verify no more duplicates
        print("\n🔍 Verifying deletion...")
        remaining_duplicates = check_duplicates(scoring_date)
        
        if not remaining_duplicates:
            print("✅ Verification passed: All duplicates removed!")
        else:
            print(f"⚠️  Warning: Still found {len(remaining_duplicates)} duplicate groups")
        
        return len(duplicates)
        
    except Exception as e:
        print(f"❌ Error deleting duplicates: {e}")
        return 0


def get_table_stats():
    """Get overall table statistics."""
    print("📊 Table Statistics\n")
    
    stats_query = """
    SELECT 
        COUNT(*) as total_rows,
        COUNT(DISTINCT user_id) as unique_users,
        COUNT(DISTINCT account_id) as unique_accounts,
        MIN(scoring_date) as earliest_date,
        MAX(scoring_date) as latest_date,
        COUNT(DISTINCT scoring_date) as total_scoring_dates
    FROM vinatien_analytics.ewa_eligibility_scores
    """
    
    stats = execute_clickhouse_query(stats_query)
    
    if stats:
        s = stats[0]
        print(f"Total Rows: {s['total_rows']:,}")
        print(f"Unique Users: {s['unique_users']:,}")
        print(f"Unique Accounts: {s['unique_accounts']:,}")
        print(f"Date Range: {s['earliest_date']} to {s['latest_date']}")
        print(f"Total Scoring Dates: {s['total_scoring_dates']}")
        
        # Calculate expected vs actual rows
        expected_rows = s['unique_users'] * s['total_scoring_dates']
        duplicate_percentage = ((s['total_rows'] - expected_rows) / s['total_rows'] * 100) if s['total_rows'] > 0 else 0
        
        if duplicate_percentage > 0:
            print(f"\n⚠️  Duplicate Rate: ~{duplicate_percentage:.1f}% ({s['total_rows'] - expected_rows:,} extra rows)")
        else:
            print(f"\n✅ Data Quality: Good (no apparent duplicates)")
    
    # Recent dates distribution
    print("\n📅 Recent Scoring Dates:\n")
    recent_query = """
    SELECT 
        scoring_date,
        COUNT(*) as record_count,
        COUNT(DISTINCT user_id) as unique_users
    FROM vinatien_analytics.ewa_eligibility_scores
    WHERE scoring_date >= today() - 7
    GROUP BY scoring_date
    ORDER BY scoring_date DESC
    """
    
    recent = execute_clickhouse_query(recent_query)
    
    if recent:
        for r in recent:
            avg_records_per_user = r['record_count'] / r['unique_users'] if r['unique_users'] > 0 else 0
            status = "⚠️ " if avg_records_per_user > 1.1 else "✓ "
            print(f"{status}{r['scoring_date']}: {r['record_count']:,} records, "
                  f"{r['unique_users']:,} users "
                  f"(avg {avg_records_per_user:.2f} records/user)")


def main():
    parser = argparse.ArgumentParser(
        description='ClickHouse EWA Eligibility Scores Deduplication Tool'
    )
    
    parser.add_argument(
        '--check',
        action='store_true',
        help='Check for duplicate entries'
    )
    
    parser.add_argument(
        '--remove',
        action='store_true',
        help='Remove duplicate entries (keeps most recent)'
    )
    
    parser.add_argument(
        '--stats',
        action='store_true',
        help='Show table statistics'
    )
    
    parser.add_argument(
        '--date',
        type=str,
        help='Specific date to check/clean (YYYY-MM-DD)'
    )
    
    parser.add_argument(
        '--dry-run',
        action='store_true',
        help='Show what would be deleted without actually deleting'
    )
    
    args = parser.parse_args()
    
    # Validate date format if provided
    if args.date:
        try:
            datetime.strptime(args.date, '%Y-%m-%d')
        except ValueError:
            print(f"❌ Invalid date format: {args.date}. Use YYYY-MM-DD")
            return 1
    
    try:
        if args.stats:
            get_table_stats()
        
        elif args.check:
            duplicates = check_duplicates(args.date)
            return 1 if duplicates else 0
        
        elif args.remove:
            removed = remove_duplicates(args.date, args.dry_run)
            return 0 if removed >= 0 else 1
        
        else:
            # Default: show stats
            get_table_stats()
            print("\n" + "="*70)
            print("Use --check to find duplicates, --remove to delete them")
            print("Examples:")
            print("  python cleanup_duplicates.py --check")
            print("  python cleanup_duplicates.py --check --date 2026-01-31")
            print("  python cleanup_duplicates.py --remove --dry-run")
            print("  python cleanup_duplicates.py --remove")
            return 0
    
    except Exception as e:
        print(f"\n❌ Error: {e}")
        import traceback
        traceback.print_exc()
        return 1


if __name__ == "__main__":
    sys.exit(main())
