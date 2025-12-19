-- ClickHouse Analytics Queries
-- These queries demonstrate how to analyze your transaction data in ClickHouse

-- ============================================================================
-- BASIC QUERIES
-- ============================================================================

-- 1. Get total row count
SELECT count() as total_transactions 
FROM vinatien_analytics.transactions_fact;

-- 2. View recent transactions
SELECT 
    booking_date,
    bank_account_id,
    amount,
    currency,
    creditor_name,
    debtor_name
FROM vinatien_analytics.transactions_fact
ORDER BY booking_date DESC
LIMIT 10;

-- 3. Get date range of data
SELECT 
    min(booking_date) as earliest_transaction,
    max(booking_date) as latest_transaction,
    count() as total_count
FROM vinatien_analytics.transactions_fact;

-- ============================================================================
-- FINANCIAL ANALYSIS
-- ============================================================================

-- 4. Daily transaction summary
SELECT 
    toDate(booking_date) as date,
    count() as total_transactions,
    countIf(is_credit = 1) as credits_count,
    countIf(is_debit = 1) as debits_count,
    sumIf(amount, is_credit = 1) as total_credits,
    abs(sumIf(amount, is_debit = 1)) as total_debits,
    sum(amount) as net_amount
FROM vinatien_analytics.transactions_fact
GROUP BY date
ORDER BY date DESC;

-- 5. Monthly trends
SELECT 
    concat(toString(year), '-', lpad(toString(month), 2, '0')) as year_month,
    count() as transactions,
    sum(amount) as net_amount,
    sumIf(amount, is_credit = 1) as total_credits,
    abs(sumIf(amount, is_debit = 1)) as total_debits
FROM vinatien_analytics.transactions_fact
GROUP BY year, month
ORDER BY year DESC, month DESC;

-- 6. Transactions by day of week
SELECT 
    day_name,
    day_of_week,
    count() as transaction_count,
    avg(abs_amount) as avg_amount,
    sum(amount) as net_amount
FROM vinatien_analytics.transactions_fact
GROUP BY day_name, day_of_week
ORDER BY day_of_week;

-- 7. Weekend vs Weekday comparison
SELECT 
    if(is_weekend = 1, 'Weekend', 'Weekday') as day_type,
    count() as transactions,
    avg(abs_amount) as avg_amount,
    sum(amount) as net_amount
FROM vinatien_analytics.transactions_fact
GROUP BY day_type;

-- ============================================================================
-- ACCOUNT ANALYSIS
-- ============================================================================

-- 8. Top accounts by transaction volume
SELECT 
    bank_account_id,
    bank_provider,
    count() as transaction_count,
    sum(amount) as net_amount,
    sumIf(amount, is_credit = 1) as total_credits,
    abs(sumIf(amount, is_debit = 1)) as total_debits
FROM vinatien_analytics.transactions_fact
GROUP BY bank_account_id, bank_provider
ORDER BY transaction_count DESC;

-- 9. Bank provider comparison
SELECT 
    bank_provider,
    count() as transactions,
    countDistinct(bank_account_id) as unique_accounts,
    sum(amount) as net_amount,
    avg(abs_amount) as avg_transaction_amount
FROM vinatien_analytics.transactions_fact
GROUP BY bank_provider
ORDER BY transactions DESC;

-- ============================================================================
-- TIME-BASED PATTERNS
-- ============================================================================

-- 10. Hourly transaction patterns
SELECT 
    hour_of_day,
    count() as transaction_count,
    avg(abs_amount) as avg_amount
FROM vinatien_analytics.transactions_fact
WHERE hour_of_day > 0
GROUP BY hour_of_day
ORDER BY hour_of_day;

-- 11. Monthly growth rate
SELECT 
    year_month,
    transactions,
    net_amount,
    round((transactions - lag(transactions) OVER (ORDER BY year_month)) / 
          lag(transactions) OVER (ORDER BY year_month) * 100, 2) as growth_rate_pct
FROM (
    SELECT 
        concat(toString(year), '-', lpad(toString(month), 2, '0')) as year_month,
        count() as transactions,
        sum(amount) as net_amount
    FROM vinatien_analytics.transactions_fact
    GROUP BY year, month
    ORDER BY year, month
);

-- ============================================================================
-- CREDITOR/DEBTOR ANALYSIS
-- ============================================================================

-- 12. Top creditors (who receives money)
SELECT 
    creditor_name,
    count() as transaction_count,
    sum(amount) as total_received,
    avg(amount) as avg_amount
FROM vinatien_analytics.transactions_fact
WHERE creditor_name != '' AND is_credit = 1
GROUP BY creditor_name
ORDER BY total_received DESC
LIMIT 10;

-- 13. Top debtors (who sends money)
SELECT 
    debtor_name,
    count() as transaction_count,
    abs(sum(amount)) as total_sent,
    avg(abs_amount) as avg_amount
FROM vinatien_analytics.transactions_fact
WHERE debtor_name != '' AND is_debit = 1
GROUP BY debtor_name
ORDER BY total_sent DESC
LIMIT 10;

-- ============================================================================
-- AGGREGATED VIEWS (Using Pre-computed Tables)
-- ============================================================================

-- 14. Query daily summary (much faster for large datasets)
SELECT 
    date,
    sum(total_transactions) as transactions,
    sum(sum_credits) as credits,
    sum(sum_debits) as debits,
    sum(net_amount) as net
FROM vinatien_analytics.daily_summary
GROUP BY date
ORDER BY date DESC;

-- 15. Query monthly summary
SELECT 
    year_month,
    sum(total_transactions) as transactions,
    sum(sum_credits) as credits,
    sum(sum_debits) as debits,
    sum(net_amount) as net
FROM vinatien_analytics.monthly_summary
GROUP BY year_month
ORDER BY year_month DESC;

-- ============================================================================
-- ETL MONITORING
-- ============================================================================

-- 16. Check ETL batch loads
SELECT 
    etl_batch_id,
    min(etl_loaded_at) as load_time,
    count() as records_loaded
FROM vinatien_analytics.transactions_fact
GROUP BY etl_batch_id
ORDER BY load_time DESC;

-- 17. Data freshness check
SELECT 
    max(etl_loaded_at) as last_load_time,
    count() as total_records,
    countDistinct(etl_batch_id) as total_batches
FROM vinatien_analytics.transactions_fact;

-- ============================================================================
-- ADVANCED ANALYTICS
-- ============================================================================

-- 18. Moving average (7-day)
SELECT 
    date,
    transactions,
    avg(transactions) OVER (
        ORDER BY date 
        ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
    ) as moving_avg_7day
FROM (
    SELECT 
        toDate(booking_date) as date,
        count() as transactions
    FROM vinatien_analytics.transactions_fact
    GROUP BY date
    ORDER BY date
);

-- 19. Percentile analysis
SELECT 
    quantile(0.25)(abs_amount) as p25,
    quantile(0.50)(abs_amount) as median,
    quantile(0.75)(abs_amount) as p75,
    quantile(0.90)(abs_amount) as p90,
    quantile(0.95)(abs_amount) as p95,
    quantile(0.99)(abs_amount) as p99
FROM vinatien_analytics.transactions_fact;

-- 20. Cohort analysis by month
SELECT 
    first_month,
    months_since_first,
    count() as users,
    sum(transaction_count) as transactions
FROM (
    SELECT 
        bank_account_id,
        toStartOfMonth(min(booking_date)) OVER (PARTITION BY bank_account_id) as first_month,
        toStartOfMonth(booking_date) as month,
        dateDiff('month', first_month, month) as months_since_first,
        count() as transaction_count
    FROM vinatien_analytics.transactions_fact
    GROUP BY bank_account_id, month, first_month
)
GROUP BY first_month, months_since_first
ORDER BY first_month, months_since_first;
