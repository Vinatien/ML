"""
Airflow configuration for local development.
Sets up Airflow to run ETL pipelines locally.
"""

import os
from pathlib import Path

# Get the project root directory
PROJECT_ROOT = Path(__file__).parent.parent.parent

# Airflow home directory
AIRFLOW_HOME = PROJECT_ROOT / 'ml' / 'airflow'

# Core Airflow settings
os.environ['AIRFLOW_HOME'] = str(AIRFLOW_HOME)
os.environ['AIRFLOW__CORE__DAGS_FOLDER'] = str(AIRFLOW_HOME / 'dags')
os.environ['AIRFLOW__CORE__BASE_LOG_FOLDER'] = str(AIRFLOW_HOME / 'logs')
os.environ['AIRFLOW__CORE__EXECUTOR'] = 'LocalExecutor'
os.environ['AIRFLOW__CORE__LOAD_EXAMPLES'] = 'False'
os.environ['AIRFLOW__CORE__LOAD_DEFAULT_CONNECTIONS'] = 'False'

# Database connection (use PostgreSQL from Docker)
# This uses the same database as your backend
DB_HOST = os.getenv('DB_HOST', 'localhost')
DB_PORT = os.getenv('DB_PORT', '5432')
DB_NAME = 'airflow_db'  # Separate database for Airflow metadata
DB_USER = os.getenv('DB_USER', 'postgres')
DB_PASSWORD = os.getenv('DB_PASSWORD', 'root')

os.environ['AIRFLOW__DATABASE__SQL_ALCHEMY_CONN'] = (
    f'postgresql+psycopg2://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}'
)

# Web server settings
os.environ['AIRFLOW__WEBSERVER__WEB_SERVER_PORT'] = '8080'
os.environ['AIRFLOW__WEBSERVER__SECRET_KEY'] = 'your-secret-key-change-in-production'

# Scheduler settings
os.environ['AIRFLOW__SCHEDULER__DAG_DIR_LIST_INTERVAL'] = '60'  # Scan for new DAGs every 60 seconds

# Email settings (optional)
os.environ['AIRFLOW__SMTP__SMTP_HOST'] = 'smtp.gmail.com'
os.environ['AIRFLOW__SMTP__SMTP_PORT'] = '587'
os.environ['AIRFLOW__SMTP__SMTP_USER'] = 'your-email@gmail.com'
os.environ['AIRFLOW__SMTP__SMTP_PASSWORD'] = 'your-password'
os.environ['AIRFLOW__SMTP__SMTP_MAIL_FROM'] = 'airflow@vinatien.com'

print(f"Airflow Home: {AIRFLOW_HOME}")
print(f"DAGs Folder: {os.environ['AIRFLOW__CORE__DAGS_FOLDER']}")
print(f"Database: {os.environ['AIRFLOW__DATABASE__SQL_ALCHEMY_CONN']}")
