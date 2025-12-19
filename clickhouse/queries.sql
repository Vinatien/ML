-- ====================================================================
-- ClickHouse Query Examples for VinaTien Analytics
-- Database: vinatien_analytics
-- Table: transactions_fact
-- ====================================================================

-- 1. BASIC QUERIES
-- ====================================================================

-- Count all transactions
SELECT COUNT(*) as total_transactions
FROM vinatien_analytics.transactions_fact;

-- View latest 10 transactions
SELECT 
    booking_date,
    amount,
    currency,
    creditor_name,
    debtor_name,
    status,
    etl_loaded_at
FROM vinatien_analytics.transactions_fact
ORDER BY booking_date DESC
LIMIT 10;

-- View all data with key columns
SELECT 
    id,
    booking_date,
    amount,
    currency,
    status,
    creditor_name,
    debtor_name,
    is_credit,
    is_debit,
    day_name,
    month_name,
    etl_batch_id,
    etl_loaded_at
FROM vinatien_analytics.transactions_fact
ORDER BY booking_date DESC;


-- 2. AGGREGATION QUERIES
-- ====================================================================

-- Total transactions by status
SELECT 
    status,
    COUNT(*) as count,
    SUM(amount) as total_amount,
    AVG(amount) as avg_amount
FROM vinatien_analytics.transactions_fact
GROUP BY status
ORDER BY count DESC;

-- Transactions by currency
SELECT 
    currency,
    COUNT(*) as transaction_count,
    SUM(amount) as total_amount,
    MIN(amount) as min_amount,
    MAX(amount) as max_amount,
    AVG(amount) as avg_amount
FROM vinatien_analytics.transactions_fact
GROUP BY currency
ORDER BY total_amount DESC;

-- Credit vs Debit analysis
SELECT 
    CASE 
        WHEN is_credit = 1 THEN 'Credit (Money In)'
        WHEN is_debit = 1 THEN 'Debit (Money Out)'
        ELSE 'Other'
    END as transaction_direction,
    COUNT(*) as count,
    SUM(abs_amount) as total_amount,
    AVG(abs_amount) as avg_amount
FROM vinatien_analytics.transactions_fact
GROUP BY is_credit, is_debit
ORDER BY total_amount DESC;


-- 3. TIME-BASED ANALYSIS
-- ====================================================================

-- Transactions by day of week
SELECT 
    day_name,
    day_of_week,
    COUNT(*) as transaction_count,
    SUM(amount) as total_amount,
    AVG(amount) as avg_amount
FROM vinatien_analytics.transactions_fact
GROUP BY day_name, day_of_week
ORDER BY day_of_week;

-- Transactions by month
SELECT 
    year,
    month,
    month_name,
    COUNT(*) as transaction_count,
    SUM(amount) as total_amount
FROM vinatien_analytics.transactions_fact
GROUP BY year, month, month_name
ORDER BY year DESC, month DESC;

-- Weekend vs Weekday analysis
SELECT 
    CASE WHEN is_weekend = 1 THEN 'Weekend' ELSE 'Weekday' END as period_type,
    COUNT(*) as transaction_count,
    SUM(amount) as total_amount,
    AVG(amount) as avg_amount
FROM vinatien_analytics.transactions_fact
GROUP BY is_weekend
ORDER BY is_weekend;

-- Hourly transaction pattern
SELECT 
    hour_of_day,
    COUNT(*) as transaction_count,
    SUM(amount) as total_amount,
    AVG(amount) as avg_amount
FROM vinatien_analytics.transactions_fact
GROUP BY hour_of_day
ORDER BY hour_of_day;


-- 4. BANK ACCOUNT ANALYSIS
-- ====================================================================

-- Transactions by bank account
SELECT 
    bank_account_id,
    COUNT(*) as transaction_count,
    SUM(amount) as total_amount,
    MIN(booking_date) as first_transaction,
    MAX(booking_date) as last_transaction
FROM vinatien_analytics.transactions_fact
GROUP BY bank_account_id
ORDER BY transaction_count DESC;

-- Bank provider analysis
SELECT 
    bank_provider,
    COUNT(*) as transaction_count,
    SUM(amount) as total_amount,
    AVG(amount) as avg_amount
FROM vinatien_analytics.transactions_fact
GROUP BY bank_provider
ORDER BY transaction_count DESC;


-- 5. COUNTERPARTY ANALYSIS
-- ====================================================================

-- Top creditors (money received from)
SELECT 
    creditor_name,
    COUNT(*) as transaction_count,
    SUM(amount) as total_received
FROM vinatien_analytics.transactions_fact
WHERE creditor_name != ''
GROUP BY creditor_name
ORDER BY total_received DESC
LIMIT 10;

-- Top debtors (money sent to)
SELECT 
    debtor_name,
    COUNT(*) as transaction_count,
    SUM(ABS(amount)) as total_sent
FROM vinatien_analytics.transactions_fact
WHERE debtor_name != ''
GROUP BY debtor_name
ORDER BY total_sent DESC
LIMIT 10;


-- 6. ETL MONITORING QUERIES
-- ====================================================================

-- ETL batch summary
SELECT 
    etl_batch_id,
    COUNT(*) as records_in_batch,
    MIN(booking_date) as earliest_transaction,
    MAX(booking_date) as latest_transaction,
    MIN(etl_loaded_at) as loaded_at
FROM vinatien_analytics.transactions_fact
GROUP BY etl_batch_id
ORDER BY loaded_at DESC;

-- Daily ETL load statistics
SELECT 
    toDate(etl_loaded_at) as load_date,
    COUNT(DISTINCT etl_batch_id) as number_of_batches,
    COUNT(*) as total_records_loaded
FROM vinatien_analytics.transactions_fact
GROUP BY load_date
ORDER BY load_date DESC;

-- Check for duplicate transactions
SELECT 
    id,
    COUNT(*) as occurrence_count
FROM vinatien_analytics.transactions_fact
GROUP BY id
HAVING occurrence_count > 1
ORDER BY occurrence_count DESC;


-- 7. DATE RANGE QUERIES
-- ====================================================================

-- Transactions in last 7 days
SELECT 
    booking_date,
    COUNT(*) as transaction_count,
    SUM(amount) as total_amount
FROM vinatien_analytics.transactions_fact
WHERE booking_date >= today() - INTERVAL 7 DAY
GROUP BY booking_date
ORDER BY booking_date DESC;

-- Transactions in current month
SELECT 
    booking_date,
    amount,
    creditor_name,
    debtor_name,
    status
FROM vinatien_analytics.transactions_fact
WHERE toYYYYMM(booking_date) = toYYYYMM(today())
ORDER BY booking_date DESC;

-- Transactions by date range
SELECT 
    booking_date,
    COUNT(*) as count,
    SUM(amount) as total
FROM vinatien_analytics.transactions_fact
WHERE booking_date BETWEEN '2025-12-01' AND '2025-12-31'
GROUP BY booking_date
ORDER BY booking_date;


-- 8. ADVANCED ANALYTICS
-- ====================================================================

-- Running total by date
SELECT 
    booking_date,
    amount,
    SUM(amount) OVER (ORDER BY booking_date) as running_total
FROM vinatien_analytics.transactions_fact
ORDER BY booking_date;

-- Average transaction by day of week and hour
SELECT 
    day_name,
    hour_of_day,
    COUNT(*) as transaction_count,
    AVG(abs_amount) as avg_transaction_amount
FROM vinatien_analytics.transactions_fact
GROUP BY day_name, day_of_week, hour_of_day
ORDER BY day_of_week, hour_of_day;

-- Transaction velocity (transactions per day)
SELECT 
    toDate(booking_date) as date,
    COUNT(*) as transactions_per_day,
    SUM(amount) as daily_total,
    AVG(amount) as daily_avg
FROM vinatien_analytics.transactions_fact
GROUP BY date
ORDER BY date DESC;


-- 9. DATA QUALITY CHECKS
-- ====================================================================

-- Check for null or empty values
SELECT 
    'Missing creditor_name' as check_type,
    COUNT(*) as count
FROM vinatien_analytics.transactions_fact
WHERE creditor_name = '' OR creditor_name IS NULL

UNION ALL

SELECT 
    'Missing debtor_name' as check_type,
    COUNT(*) as count
FROM vinatien_analytics.transactions_fact
WHERE debtor_name = '' OR debtor_name IS NULL

UNION ALL

SELECT 
    'Zero amount' as check_type,
    COUNT(*) as count
FROM vinatien_analytics.transactions_fact
WHERE amount = 0;

-- Check status distribution
SELECT 
    status,
    COUNT(*) as count,
    COUNT(*) * 100.0 / (SELECT COUNT(*) FROM vinatien_analytics.transactions_fact) as percentage
FROM vinatien_analytics.transactions_fact
GROUP BY status
ORDER BY count DESC;


-- 10. EXPORT QUERIES
-- ====================================================================

-- Full data export (CSV format)
SELECT *
FROM vinatien_analytics.transactions_fact
ORDER BY booking_date DESC
FORMAT CSV;

-- Summary report
SELECT 
    'Total Transactions' as metric,
    toString(COUNT(*)) as value
FROM vinatien_analytics.transactions_fact

UNION ALL

SELECT 
    'Total Volume',
    toString(SUM(amount))
FROM vinatien_analytics.transactions_fact

UNION ALL

SELECT 
    'Average Transaction',
    toString(AVG(amount))
FROM vinatien_analytics.transactions_fact

UNION ALL

SELECT 
    'Date Range',
    concat(toString(MIN(booking_date)), ' to ', toString(MAX(booking_date)))
FROM vinatien_analytics.transactions_fact;
