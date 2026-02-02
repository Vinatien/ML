-- Migration: Convert transactions_fact to ReplacingMergeTree for native deduplication
-- This enables ClickHouse to automatically handle duplicates based on the version column

-- Step 1: Create new table with ReplacingMergeTree engine
CREATE TABLE IF NOT EXISTS vinatien_analytics.transactions_fact_new
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
    etl_batch_id String,
    
    -- Version column for ReplacingMergeTree (higher = newer)
    -- This tells ClickHouse which row to keep when deduplicating
    version UInt64
)
ENGINE = ReplacingMergeTree(version)  -- Use 'version' column to determine latest row
PARTITION BY toYYYYMM(booking_date)
ORDER BY (id, booking_date, bank_account_id)  -- id is first for deduplication key
SETTINGS index_granularity = 8192;

-- Step 2: Copy existing data from old table (if exists)
-- Assign version based on etl_loaded_at (convert to Unix timestamp)
INSERT INTO vinatien_analytics.transactions_fact_new
SELECT 
    id,
    bank_account_id,
    booking_date,
    value_date,
    amount,
    currency,
    status,
    creditor_name,
    debtor_name,
    creditor_account_last4,
    debtor_account_last4,
    created_at,
    iban,
    bank_provider,
    consent_status,
    day_of_week,
    month,
    year,
    hour_of_day,
    day_name,
    month_name,
    is_weekend,
    is_credit,
    is_debit,
    abs_amount,
    etl_loaded_at,
    etl_batch_id,
    toUnixTimestamp(etl_loaded_at) * 1000 as version  -- Convert to milliseconds for version
FROM vinatien_analytics.transactions_fact;

-- Step 3: Rename tables (atomic swap)
RENAME TABLE 
    vinatien_analytics.transactions_fact TO vinatien_analytics.transactions_fact_old,
    vinatien_analytics.transactions_fact_new TO vinatien_analytics.transactions_fact;

-- Step 4: Drop old table after verification
-- DROP TABLE vinatien_analytics.transactions_fact_old;

-- Step 5: Recreate indexes on new table
ALTER TABLE vinatien_analytics.transactions_fact ADD INDEX idx_booking_date booking_date TYPE minmax GRANULARITY 3;
ALTER TABLE vinatien_analytics.transactions_fact ADD INDEX idx_bank_account bank_account_id TYPE set(100) GRANULARITY 1;

-- Step 6: Optimize to trigger deduplication (removes old versions)
-- NOTE: This is resource-intensive, run during off-peak hours
-- OPTIMIZE TABLE vinatien_analytics.transactions_fact FINAL;

-- Verification queries:
-- 1. Check for duplicates (should show count > 1 before OPTIMIZE)
SELECT 
    id,
    COUNT(*) as duplicate_count,
    MAX(version) as latest_version
FROM vinatien_analytics.transactions_fact
GROUP BY id
HAVING COUNT(*) > 1
LIMIT 10;

-- 2. Query with FINAL to get deduplicated results
SELECT COUNT(*) FROM vinatien_analytics.transactions_fact FINAL;

-- 3. Compare counts (without FINAL shows all rows including duplicates)
SELECT 
    'With duplicates' as type, COUNT(*) as count 
FROM vinatien_analytics.transactions_fact
UNION ALL
SELECT 
    'Deduplicated' as type, COUNT(*) as count 
FROM vinatien_analytics.transactions_fact FINAL;
