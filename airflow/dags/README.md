# Airflow DAGs Directory

## Active DAGs

### ✅ postgresql_to_clickhouse_etl.py
**Status**: Active and working

**Purpose**: Complete ETL pipeline from PostgreSQL to ClickHouse analytics database

**Tasks**:
1. Check PostgreSQL connection
2. Check ClickHouse connection
3. Extract from PostgreSQL
4. Transform data (27 features)
5. Save to Parquet feature store
6. Load to ClickHouse
7. Validate and report
8. Cleanup temp files

**Schedule**: Daily at 1:00 AM

**Usage**:
- Toggle ON in Airflow UI
- Click ▶️ to trigger manually
- Monitor in Graph View

---

## Disabled DAGs

### ❌ etl_postgres_to_parquet.py.disabled
**Status**: Disabled (renamed with .disabled extension)

**Reason**: This was an old DAG with broken imports:
- Tried to import from `ml.data.fetch_transactions` (module doesn't exist)
- Caused "ModuleNotFoundError: No module named 'ml'" in Airflow UI
- Has been replaced by `postgresql_to_clickhouse_etl.py`

**If you need it**: Rename back to `.py` and fix the import paths

---

## Troubleshooting

### DAG Import Errors
If you see "DAG Import Errors" in the Airflow UI:

1. **Check the error message** - it will tell you which file and line
2. **Verify imports** - make sure all modules exist in the container
3. **Check Python path** - ensure `sys.path` includes the ML directory
4. **Restart scheduler** after fixing:
   ```bash
   docker compose -f docker-compose-airflow.yaml restart airflow-scheduler
   ```

### DAG Not Appearing
1. **Check file extension** - must be `.py` (not `.py.disabled`)
2. **Check for syntax errors** - run `python dags/your_dag.py` locally
3. **Wait 30 seconds** - Airflow scans for new DAGs periodically
4. **Check scheduler logs**:
   ```bash
   docker compose -f docker-compose-airflow.yaml logs airflow-scheduler
   ```

---

## Adding New DAGs

1. Create a new `.py` file in this directory
2. Follow the template from `postgresql_to_clickhouse_etl.py`
3. Ensure all imports are correct
4. Test locally first: `python dags/your_new_dag.py`
5. Wait for Airflow to auto-detect it (~30 seconds)
6. Check UI for import errors
7. Toggle ON and test

---

## Current Setup

- **Airflow Version**: 2.8.0 (Python 3.10)
- **Executor**: LocalExecutor
- **DAG Folder**: `/opt/airflow/dags` (mounted from this directory)
- **Config Path**: `/opt/airflow/config` (mounted from `ML/config/`)
- **Data Path**: `/opt/airflow/data` (mounted from `ML/data/`)

---

## Quick Commands

| Action | Command |
|--------|---------|
| List all DAGs | `docker exec vinatien-airflow-scheduler airflow dags list` |
| Show DAG structure | `docker exec vinatien-airflow-scheduler airflow dags show postgresql_to_clickhouse_etl` |
| Test a task | `docker exec vinatien-airflow-scheduler airflow tasks test postgresql_to_clickhouse_etl check_postgresql_connection 2024-01-01` |
| Check import errors | View "DAG Import Errors" in Airflow UI |
| Restart scheduler | `docker compose -f ../docker-compose-airflow.yaml restart airflow-scheduler` |
