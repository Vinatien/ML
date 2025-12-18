# ML Workflows for VinaTien Transaction Data

This directory contains tools and scripts for accessing and processing transaction data from the VinaTien database for machine learning workflows.

## Setup

1. **Install dependencies:**
   ```bash
   pip install -r requirements.txt
   ```

2. **Ensure Docker containers are running:**
   ```bash
   cd ../backend
   docker compose ps
   ```

## Directory Structure

```
ml/
├── config/
│   └── database.py              # Database connection configuration
├── data/
│   ├── fetch_transactions.py    # Fetch data from database
│   ├── export_transactions.py   # Export data to files
│   └── raw/                     # Exported data files
├── notebooks/                   # Jupyter notebooks for analysis
├── models/                      # ML model implementations
└── ml_services/                # ML services and APIs
```

## Quick Start

### 1. Test Database Connection

```python
from config.database import test_connection
test_connection()
```

### 2. Fetch All Transactions

```python
from data.fetch_transactions import fetch_all_transactions
df = fetch_all_transactions()
print(df.head())
```

### 3. Fetch Recent Transactions (Last 30 Days)

```python
from data.fetch_transactions import fetch_recent_transactions
df = fetch_recent_transactions(days=30)
```

### 4. Prepare ML Dataset

```python
from data.fetch_transactions import prepare_ml_dataset
df_ml = prepare_ml_dataset(filter_status='booked')
```

### 5. Export Data

```python
from data.export_transactions import export_all_formats
files = export_all_formats(df, "my_export")
```

## Database Schema

### Transactions Table

The main `transactions` table contains:

- **id**: Transaction ID (primary key)
- **booking_date**: Date when transaction was booked
- **value_date**: Value date of transaction
- **amount**: Transaction amount (positive = credit, negative = debit)
- **currency**: Currency code (e.g., 'EUR')
- **booking_status**: 'booked' or 'pending'
- **creditor_name**: Name of recipient
- **debtor_name**: Name of sender
- **creditor_account_last4**: Last 4 digits of recipient account
- **debtor_account_last4**: Last 4 digits of sender account
- **bank_account_id**: Foreign key to bank_accounts table
- **created_at**: Timestamp when record was created

## Database Connection Parameters

The database connection uses these parameters (configured in `config/database.py`):

- **Host**: localhost
- **Port**: 5432
- **Database**: vinatien_db
- **User**: postgres
- **Password**: root

These match your Docker Compose setup in `backend/docker-compose.yaml`.

## Example Scripts

### Fetch and Save All Data

```bash
cd ml
python data/export_transactions.py
```

### Interactive Data Exploration

```bash
cd ml
python data/fetch_transactions.py
```

### Use in Your Own Scripts

```python
import sys
from pathlib import Path
sys.path.append(str(Path(__file__).parent / 'ml'))

from config.database import execute_query

# Custom query
query = """
    SELECT * FROM transactions 
    WHERE amount > 1000 
    AND booking_status = 'booked'
    ORDER BY booking_date DESC
"""

df = execute_query(query)
print(df)
```

## Notes

- All data is fetched from the PostgreSQL database running in Docker
- The database contains real transaction data synced from VPBank API
- Data is persisted in Docker volumes, so it survives container restarts
- Always ensure Docker containers are running before accessing the database
