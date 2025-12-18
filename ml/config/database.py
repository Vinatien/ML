"""
Database configuration for ML workflows.
Contains database connection settings and helper functions.
"""

import os
from typing import Optional
import psycopg2
from psycopg2.extras import RealDictCursor
import pandas as pd


class DatabaseConfig:
    """Database configuration class."""
    
    # Database connection parameters
    # These match your Docker Compose setup
    DB_HOST = os.getenv("DB_HOST", "localhost")
    DB_PORT = os.getenv("DB_PORT", "5432")
    DB_NAME = os.getenv("DB_NAME", "vinatien_db")
    DB_USER = os.getenv("DB_USER", "postgres")
    DB_PASSWORD = os.getenv("DB_PASSWORD", "root")
    
    @classmethod
    def get_connection_string(cls) -> str:
        """Get PostgreSQL connection string."""
        return f"postgresql://{cls.DB_USER}:{cls.DB_PASSWORD}@{cls.DB_HOST}:{cls.DB_PORT}/{cls.DB_NAME}"
    
    @classmethod
    def get_connection(cls):
        """Create a database connection."""
        return psycopg2.connect(
            host=cls.DB_HOST,
            port=cls.DB_PORT,
            database=cls.DB_NAME,
            user=cls.DB_USER,
            password=cls.DB_PASSWORD
        )
    
    @classmethod
    def get_dict_connection(cls):
        """Create a database connection that returns dictionaries."""
        return psycopg2.connect(
            host=cls.DB_HOST,
            port=cls.DB_PORT,
            database=cls.DB_NAME,
            user=cls.DB_USER,
            password=cls.DB_PASSWORD,
            cursor_factory=RealDictCursor
        )


def execute_query(query: str, params: Optional[tuple] = None) -> pd.DataFrame:
    """
    Execute a SQL query and return results as a pandas DataFrame.
    
    Args:
        query: SQL query string
        params: Optional tuple of query parameters
        
    Returns:
        pandas DataFrame with query results
    """
    conn = DatabaseConfig.get_connection()
    try:
        df = pd.read_sql_query(query, conn, params=params)
        return df
    finally:
        conn.close()


def test_connection() -> bool:
    """
    Test database connection.
    
    Returns:
        True if connection successful, False otherwise
    """
    try:
        conn = DatabaseConfig.get_connection()
        cursor = conn.cursor()
        cursor.execute("SELECT 1")
        cursor.close()
        conn.close()
        print("✅ Database connection successful!")
        return True
    except Exception as e:
        print(f"❌ Database connection failed: {e}")
        return False


if __name__ == "__main__":
    # Test the connection
    test_connection()
