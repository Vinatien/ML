"""
Export transaction data to various formats (CSV, Parquet, JSON).
Useful for sharing data with other tools or team members.
"""

import sys
from pathlib import Path
from typing import Optional
import pandas as pd
from datetime import datetime

# Add parent directory to path
sys.path.append(str(Path(__file__).parent.parent))
from data.fetch_transactions import (
    fetch_all_transactions,
    fetch_transactions_by_date_range,
    prepare_ml_dataset
)


# Output directory
OUTPUT_DIR = Path(__file__).parent / "raw"
OUTPUT_DIR.mkdir(exist_ok=True)


def export_to_csv(
    df: pd.DataFrame, 
    filename: Optional[str] = None
) -> Path:
    """
    Export DataFrame to CSV.
    
    Args:
        df: DataFrame to export
        filename: Optional custom filename
        
    Returns:
        Path to exported file
    """
    if filename is None:
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        filename = f"transactions_{timestamp}.csv"
    
    filepath = OUTPUT_DIR / filename
    df.to_csv(filepath, index=False)
    print(f"✅ Exported to CSV: {filepath}")
    return filepath


def export_to_parquet(
    df: pd.DataFrame, 
    filename: Optional[str] = None
) -> Path:
    """
    Export DataFrame to Parquet (more efficient for large datasets).
    
    Args:
        df: DataFrame to export
        filename: Optional custom filename
        
    Returns:
        Path to exported file
    """
    if filename is None:
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        filename = f"transactions_{timestamp}.parquet"
    
    filepath = OUTPUT_DIR / filename
    df.to_parquet(filepath, index=False)
    print(f"✅ Exported to Parquet: {filepath}")
    return filepath


def export_to_json(
    df: pd.DataFrame, 
    filename: Optional[str] = None
) -> Path:
    """
    Export DataFrame to JSON.
    
    Args:
        df: DataFrame to export
        filename: Optional custom filename
        
    Returns:
        Path to exported file
    """
    if filename is None:
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        filename = f"transactions_{timestamp}.json"
    
    filepath = OUTPUT_DIR / filename
    df.to_json(filepath, orient='records', date_format='iso', indent=2)
    print(f"✅ Exported to JSON: {filepath}")
    return filepath


def export_all_formats(df: pd.DataFrame, base_name: str = "transactions") -> dict:
    """
    Export DataFrame to all formats.
    
    Args:
        df: DataFrame to export
        base_name: Base name for files
        
    Returns:
        Dictionary with format names and file paths
    """
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    
    exports = {
        'csv': export_to_csv(df, f"{base_name}_{timestamp}.csv"),
        'parquet': export_to_parquet(df, f"{base_name}_{timestamp}.parquet"),
        'json': export_to_json(df, f"{base_name}_{timestamp}.json")
    }
    
    return exports


if __name__ == "__main__":
    print("\n" + "="*60)
    print("TRANSACTION DATA EXPORTER")
    print("="*60 + "\n")
    
    # Fetch data
    print("Fetching transaction data...")
    df = fetch_all_transactions()
    
    # Export to all formats
    print("\nExporting to all formats...")
    files = export_all_formats(df, "transactions_all")
    
    print("\n" + "="*60)
    print("EXPORT COMPLETE")
    print("="*60)
    print("\nExported files:")
    for format_name, filepath in files.items():
        print(f"  {format_name.upper()}: {filepath}")
    
    # Also export ML-ready dataset
    print("\n\nExporting ML-ready dataset...")
    df_ml = prepare_ml_dataset(filter_status='booked')
    ml_files = export_all_formats(df_ml, "transactions_ml")
    
    print("\nML dataset files:")
    for format_name, filepath in ml_files.items():
        print(f"  {format_name.upper()}: {filepath}")
