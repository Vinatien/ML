-- ClickHouse initialization script
-- This creates the initial database and tables

-- Create database
CREATE DATABASE IF NOT EXISTS vinatien_analytics;

-- Switch to the database
USE vinatien_analytics;

-- Create transactions fact table (optimized for analytics)
CREATE TABLE IF NOT EXISTS transactions_fact
(
    id UInt32,
    bank_account_id UInt32,
    booking_date DateTime,
    value_date DateTime,
    amount Decimal(15, 2),
    currency String,
    status String,
    creditor_name String,
    debtor_name String,
    creditor_account_last4 String,
    debtor_account_last4 String,
    created_at DateTime,
    
    -- Bank account dimensions
    iban String,
    bank_provider String,
    consent_status String,
    
    -- Derived time features
    day_of_week UInt8,
    month UInt8,
    year UInt16,
    hour_of_day UInt8,
    day_name String,
    month_name String,
    is_weekend UInt8,
    
    -- Transaction type features
    is_credit UInt8,
    is_debit UInt8,
    abs_amount Decimal(15, 2),
    
    -- ETL metadata
    etl_loaded_at DateTime DEFAULT now(),
    etl_batch_id String
)
ENGINE = MergeTree()
PARTITION BY toYYYYMM(booking_date)
ORDER BY (booking_date, bank_account_id, id)
SETTINGS index_granularity = 8192;

-- Create aggregated daily summary table
CREATE TABLE IF NOT EXISTS daily_summary
(
    date Date,
    bank_account_id UInt32,
    bank_provider String,
    total_transactions UInt32,
    total_credits UInt32,
    total_debits UInt32,
    sum_credits Decimal(18, 2),
    sum_debits Decimal(18, 2),
    net_amount Decimal(18, 2),
    avg_transaction_amount Decimal(18, 2),
    min_amount Decimal(15, 2),
    max_amount Decimal(15, 2),
    created_at DateTime DEFAULT now()
)
ENGINE = SummingMergeTree()
PARTITION BY toYYYYMM(date)
ORDER BY (date, bank_account_id)
SETTINGS index_granularity = 8192;

-- Create monthly summary table
CREATE TABLE IF NOT EXISTS monthly_summary
(
    year_month String,
    bank_account_id UInt32,
    bank_provider String,
    total_transactions UInt32,
    total_credits UInt32,
    total_debits UInt32,
    sum_credits Decimal(18, 2),
    sum_debits Decimal(18, 2),
    net_amount Decimal(18, 2),
    avg_transaction_amount Decimal(18, 2),
    created_at DateTime DEFAULT now()
)
ENGINE = SummingMergeTree()
PARTITION BY substring(year_month, 1, 4)
ORDER BY (year_month, bank_account_id)
SETTINGS index_granularity = 8192;

-- Create materialized view for automatic daily aggregation
CREATE MATERIALIZED VIEW IF NOT EXISTS daily_summary_mv
TO daily_summary
AS
SELECT
    toDate(booking_date) as date,
    bank_account_id,
    bank_provider,
    count() as total_transactions,
    countIf(is_credit = 1) as total_credits,
    countIf(is_debit = 1) as total_debits,
    sumIf(amount, is_credit = 1) as sum_credits,
    abs(sumIf(amount, is_debit = 1)) as sum_debits,
    sum(amount) as net_amount,
    avg(amount) as avg_transaction_amount,
    min(amount) as min_amount,
    max(amount) as max_amount,
    now() as created_at
FROM transactions_fact
GROUP BY date, bank_account_id, bank_provider;

-- Create indexes for common queries
-- Skipping index for faster date range queries
ALTER TABLE transactions_fact ADD INDEX idx_booking_date booking_date TYPE minmax GRANULARITY 3;
ALTER TABLE transactions_fact ADD INDEX idx_bank_account bank_account_id TYPE set(100) GRANULARITY 1;

-- Grant permissions
-- Note: User permissions are handled by environment variables in docker-compose
