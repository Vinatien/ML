"""
ClickHouse database configuration for analytics.
Contains ClickHouse connection settings and helper functions.
"""

import os
from typing import Optional, Dict, Any
from dotenv import load_dotenv
import pandas as pd

# Load environment variables
load_dotenv()

try:
    from clickhouse_driver import Client
    CLICKHOUSE_AVAILABLE = True
except ImportError:
    CLICKHOUSE_AVAILABLE = False
    print("⚠️  Warning: clickhouse-driver not installed. Run: pip install clickhouse-driver")


class ClickHouseConfig:
    """ClickHouse configuration class."""
    
    # ClickHouse connection parameters
    CH_HOST = os.getenv("CLICKHOUSE_HOST", "localhost")
    CH_PORT = int(os.getenv("CLICKHOUSE_PORT", "9000"))
    CH_HTTP_PORT = int(os.getenv("CLICKHOUSE_HTTP_PORT", "8123"))
    CH_DATABASE = os.getenv("CLICKHOUSE_DATABASE", "vinatien_analytics")
    CH_USER = os.getenv("CLICKHOUSE_USER", "clickhouse")
    CH_PASSWORD = os.getenv("CLICKHOUSE_PASSWORD", "clickhouse123")

    @classmethod
    def get_connection(cls) -> Optional[Client]:
        """
        Create a ClickHouse client connection.
        
        Returns:
            ClickHouse Client instance or None if driver not available
        """
        if not CLICKHOUSE_AVAILABLE:
            raise ImportError("clickhouse-driver not installed")
        
        return Client(
            host=cls.CH_HOST,
            port=cls.CH_PORT,
            database=cls.CH_DATABASE,
            user=cls.CH_USER,
            password=cls.CH_PASSWORD
        )
    
    @classmethod
    def get_connection_string(cls) -> str:
        """Get ClickHouse connection string for SQLAlchemy."""
        return f"clickhouse://{cls.CH_USER}:{cls.CH_PASSWORD}@{cls.CH_HOST}:{cls.CH_HTTP_PORT}/{cls.CH_DATABASE}"
    
    @classmethod
    def get_http_url(cls) -> str:
        """Get ClickHouse HTTP interface URL."""
        return f"http://{cls.CH_HOST}:{cls.CH_HTTP_PORT}"


def execute_query(query: str, params: Optional[Dict[str, Any]] = None) -> pd.DataFrame:
    """
    Execute a ClickHouse query and return results as DataFrame.
    
    Args:
        query: SQL query string
        params: Optional dictionary of query parameters
        
    Returns:
        pandas DataFrame with query results
    """
    if not CLICKHOUSE_AVAILABLE:
        raise ImportError("clickhouse-driver not installed. Run: pip install clickhouse-driver")
    
    client = ClickHouseConfig.get_connection()
    try:
        result = client.execute(query, params or {}, with_column_types=True)
        
        # Extract data and column names
        data, columns_with_types = result[0], result[1]
        columns = [col[0] for col in columns_with_types]
        
        # Create DataFrame
        df = pd.DataFrame(data, columns=columns)
        return df
    finally:
        client.disconnect()


def insert_dataframe(df: pd.DataFrame, table: str, database: Optional[str] = None) -> int:
    """
    Insert a pandas DataFrame into ClickHouse table.
    
    Args:
        df: pandas DataFrame to insert
        table: Target table name
        database: Optional database name (uses default if not provided)
        
    Returns:
        Number of rows inserted
    """
    if not CLICKHOUSE_AVAILABLE:
        raise ImportError("clickhouse-driver not installed. Run: pip install clickhouse-driver")
    
    client = ClickHouseConfig.get_connection()
    
    try:
        # Prepare the full table name
        full_table = f"{database}.{table}" if database else table
        
        # Convert DataFrame to list of tuples
        data = [tuple(row) for row in df.values]
        
        # Insert data
        client.execute(f"INSERT INTO {full_table} VALUES", data)
        
        return len(data)
    finally:
        client.disconnect()


def test_connection() -> bool:
    """
    Test ClickHouse connection.
    
    Returns:
        True if connection successful, False otherwise
    """
    if not CLICKHOUSE_AVAILABLE:
        print("❌ clickhouse-driver not installed")
        print("   Install with: pip install clickhouse-driver")
        return False
    
    try:
        client = ClickHouseConfig.get_connection()
        result = client.execute("SELECT 1")
        client.disconnect()
        
        print("✅ ClickHouse connection successful!")
        print(f"   Host: {ClickHouseConfig.CH_HOST}:{ClickHouseConfig.CH_PORT}")
        print(f"   Database: {ClickHouseConfig.CH_DATABASE}")
        print(f"   User: {ClickHouseConfig.CH_USER}")
        return True
    except Exception as e:
        print(f"❌ ClickHouse connection failed: {e}")
        return False


def get_table_info(table: str) -> pd.DataFrame:
    """
    Get information about a ClickHouse table.
    
    Args:
        table: Table name
        
    Returns:
        DataFrame with table schema information
    """
    query = f"DESCRIBE TABLE {table}"
    return execute_query(query)


def get_table_count(table: str) -> int:
    """
    Get row count for a table.
    
    Args:
        table: Table name
        
    Returns:
        Number of rows in the table
    """
    query = f"SELECT count() FROM {table}"
    result = execute_query(query)
    return int(result.iloc[0, 0])


if __name__ == "__main__":
    # Test the connection
    test_connection()
