"""
Script to initialize Airflow for local development.
Run this once to set up Airflow database and create admin user.
"""

import subprocess
import sys
from pathlib import Path

# Add airflow config to path
sys.path.insert(0, str(Path(__file__).parent))
import airflow_config

def run_command(cmd, description):
    """Run a shell command and handle errors."""
    print(f"\n{'='*60}")
    print(f"{description}")
    print(f"{'='*60}")
    print(f"Running: {cmd}\n")
    
    result = subprocess.run(cmd, shell=True, capture_output=False, text=True)
    
    if result.returncode != 0:
        print(f"❌ Error: {description} failed")
        return False
    
    print(f"✅ {description} completed successfully")
    return True


def main():
    """Initialize Airflow."""
    print("""
    ╔═══════════════════════════════════════════════════════════╗
    ║        AIRFLOW INITIALIZATION FOR VINATIEN ML             ║
    ╚═══════════════════════════════════════════════════════════╝
    """)
    
    # Step 1: Create Airflow database in PostgreSQL
    print("\n📦 Step 1: Creating Airflow database in PostgreSQL...")
    create_db_cmd = f"""
    docker exec -i vinatien-postgres psql -U postgres -c "
    CREATE DATABASE airflow_db;
    " || echo "Database might already exist"
    """
    run_command(create_db_cmd, "Create Airflow database")
    
    # Step 2: Initialize Airflow database schema
    print("\n📦 Step 2: Initializing Airflow database schema...")
    if not run_command("airflow db init", "Initialize Airflow DB"):
        print("⚠️  Database might already be initialized")
    
    # Step 3: Create admin user
    print("\n👤 Step 3: Creating Airflow admin user...")
    create_user_cmd = """
    airflow users create \
        --username admin \
        --firstname Admin \
        --lastname User \
        --role Admin \
        --email admin@vinatien.com \
        --password admin
    """
    if not run_command(create_user_cmd, "Create admin user"):
        print("⚠️  Admin user might already exist")
    
    # Step 4: Create directories
    print("\n📁 Step 4: Creating required directories...")
    airflow_home = Path(airflow_config.AIRFLOW_HOME)
    (airflow_home / 'logs').mkdir(parents=True, exist_ok=True)
    (airflow_home / 'plugins').mkdir(parents=True, exist_ok=True)
    print("✅ Directories created")
    
    print("""
    
    ╔═══════════════════════════════════════════════════════════╗
    ║                 SETUP COMPLETE! 🎉                        ║
    ╚═══════════════════════════════════════════════════════════╝
    
    Next steps:
    
    1. Start Airflow webserver:
       $ airflow webserver --port 8080
    
    2. In another terminal, start the scheduler:
       $ airflow scheduler
    
    3. Access Airflow UI:
       http://localhost:8080
       
       Login credentials:
       Username: admin
       Password: admin
    
    4. Your ETL DAG should appear in the UI:
       - postgresql_to_feature_store_etl
    
    5. Enable and trigger the DAG to start ETL pipeline!
    
    ╔═══════════════════════════════════════════════════════════╗
    ║  For AWS Lambda deployment, see aws_lambda_trigger.py     ║
    ╚═══════════════════════════════════════════════════════════╝
    """)


if __name__ == "__main__":
    main()
