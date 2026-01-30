-- ================================================================
-- ClickHouse Schema: EWA Eligibility Scores
-- ================================================================
-- 
-- Purpose: Store daily batch heuristic scoring results for:
-- - Portfolio risk monitoring
-- - Pre-qualification campaigns  
-- - Behavioral pattern analysis
-- - Approval rate trends
--
-- Updated by: ewa_batch_scoring_dag.py
-- Query frequency: Daily batch insert, hourly analytics queries
-- ================================================================

-- Drop existing table (use with caution in production!)
-- DROP TABLE IF EXISTS vinatien_analytics.ewa_eligibility_scores;

-- Main batch scoring results table
CREATE TABLE IF NOT EXISTS vinatien_analytics.ewa_eligibility_scores
(
    -- Primary keys
    scoring_date Date,
    user_id Int64,
    account_id Int64,
    
    -- Decision outputs
    approved UInt8,                     -- 1 = approved, 0 = rejected
    max_advance_amount Float64,         -- Maximum EWA amount user can borrow
    risk_tier String,                   -- A, B, C, D, E
    risk_score Float64,                 -- 0-100, higher = riskier
    
    -- Tree A: Basic Eligibility
    basic_eligibility_passed UInt8,     -- 1 = passed all gates
    
    -- Tree B: Income & Payday Inference
    has_payroll_pattern UInt8,          -- 1 = detected regular income
    estimated_monthly_income Float64,   -- Estimated monthly income from transactions
    payday_pattern String,              -- 'weekly', 'biweekly', 'monthly', 'unknown'
    days_to_payday Int32,               -- Days until next estimated payday
    
    -- Tree C: Risk Tier Assignment
    behavioral_flags Array(String),     -- e.g., ['nsf_high:3', 'rapid_redraw']
    pattern_type String,                -- 'irregular_healthy', 'regular_suspicious', 'first_time'
    appears_legitimate_use UInt8,       -- 1 = usage pattern matches emergency spending
    
    -- Tree D: Advance Limit Calculation
    accrued_wage Float64,               -- Wages earned since last payday
    percentage_allowed Float64,         -- % of accrued wage allowed (based on risk tier)
    fee_amount Float64,                 -- Fee for this advance (flat fee by tier)
    
    -- Tree E: Cooldown
    cooldown_allowed UInt8,             -- 1 = cooldown period satisfied
    hours_until_next_advance Nullable(Float64),  -- Hours remaining if in cooldown
    
    -- Explainability & Metadata
    decision_path Array(String),        -- Step-by-step reasoning (e.g., ['Tree A passed', 'Tree B: biweekly pattern'])
    rejection_reason Nullable(String),  -- Human-readable rejection reason
    scoring_timestamp DateTime,         -- Exact timestamp of scoring
    etl_batch_id String                 -- Batch ID for tracking ETL runs
)
ENGINE = MergeTree()
PARTITION BY toYYYYMM(scoring_date)
ORDER BY (scoring_date, user_id)
SETTINGS index_granularity = 8192;

-- ================================================================
-- Indexes for common query patterns
-- ================================================================

-- Index for user-level queries (lookup specific user's scoring history)
-- Already covered by ORDER BY (scoring_date, user_id)

-- Index for tier-based aggregations
ALTER TABLE vinatien_analytics.ewa_eligibility_scores 
ADD INDEX idx_risk_tier (risk_tier) TYPE minmax GRANULARITY 4;

-- Index for approval status filtering
ALTER TABLE vinatien_analytics.ewa_eligibility_scores
ADD INDEX idx_approved (approved) TYPE set(2) GRANULARITY 4;

-- Index for pattern type analysis
ALTER TABLE vinatien_analytics.ewa_eligibility_scores
ADD INDEX idx_pattern_type (pattern_type) TYPE set(10) GRANULARITY 4;

-- ================================================================
-- Materialized Views for Common Analytics
-- ================================================================

-- Daily approval rate by tier
CREATE MATERIALIZED VIEW IF NOT EXISTS vinatien_analytics.ewa_daily_approval_rate_by_tier
ENGINE = SummingMergeTree()
PARTITION BY toYYYYMM(scoring_date)
ORDER BY (scoring_date, risk_tier)
AS
SELECT 
    scoring_date,
    risk_tier,
    COUNT(*) as total_requests,
    SUM(approved) as total_approvals,
    SUM(max_advance_amount) as total_max_advance_amount,
    AVG(risk_score) as avg_risk_score
FROM vinatien_analytics.ewa_eligibility_scores
GROUP BY scoring_date, risk_tier;

-- Daily behavioral pattern distribution
CREATE MATERIALIZED VIEW IF NOT EXISTS vinatien_analytics.ewa_daily_pattern_distribution
ENGINE = SummingMergeTree()
PARTITION BY toYYYYMM(scoring_date)
ORDER BY (scoring_date, pattern_type)
AS
SELECT 
    scoring_date,
    pattern_type,
    appears_legitimate_use,
    COUNT(*) as user_count,
    SUM(approved) as approved_count,
    AVG(risk_score) as avg_risk_score
FROM vinatien_analytics.ewa_eligibility_scores
GROUP BY scoring_date, pattern_type, appears_legitimate_use;

-- ================================================================
-- Common Queries (Examples)
-- ================================================================

-- Query 1: Today's approval rate by tier
-- SELECT 
--     risk_tier,
--     COUNT(*) as total,
--     SUM(approved) as approvals,
--     ROUND(100.0 * SUM(approved) / COUNT(*), 2) as approval_rate_pct
-- FROM vinatien_analytics.ewa_eligibility_scores
-- WHERE scoring_date = today()
-- GROUP BY risk_tier
-- ORDER BY risk_tier;

-- Query 2: Average max advance by tier
-- SELECT 
--     risk_tier,
--     ROUND(AVG(max_advance_amount), 2) as avg_max_advance,
--     ROUND(quantile(0.5)(max_advance_amount), 2) as median_max_advance,
--     ROUND(quantile(0.9)(max_advance_amount), 2) as p90_max_advance
-- FROM vinatien_analytics.ewa_eligibility_scores
-- WHERE approved = 1 AND scoring_date >= today() - 30
-- GROUP BY risk_tier;

-- Query 3: Top rejection reasons (last 7 days)
-- SELECT 
--     rejection_reason,
--     COUNT(*) as count,
--     ROUND(100.0 * COUNT(*) / SUM(COUNT(*)) OVER (), 2) as percentage
-- FROM vinatien_analytics.ewa_eligibility_scores
-- WHERE approved = 0 AND scoring_date >= today() - 7
-- GROUP BY rejection_reason
-- ORDER BY count DESC
-- LIMIT 10;

-- Query 4: Behavioral pattern analysis
-- SELECT 
--     pattern_type,
--     appears_legitimate_use,
--     COUNT(*) as user_count,
--     SUM(approved) as approved_count,
--     ROUND(AVG(risk_score), 2) as avg_risk_score,
--     ROUND(AVG(max_advance_amount), 2) as avg_max_advance
-- FROM vinatien_analytics.ewa_eligibility_scores
-- WHERE scoring_date = today()
-- GROUP BY pattern_type, appears_legitimate_use
-- ORDER BY user_count DESC;

-- Query 5: User scoring history (for specific user)
-- SELECT 
--     scoring_date,
--     approved,
--     risk_tier,
--     risk_score,
--     max_advance_amount,
--     pattern_type,
--     rejection_reason
-- FROM vinatien_analytics.ewa_eligibility_scores
-- WHERE user_id = 123
-- ORDER BY scoring_date DESC
-- LIMIT 30;

-- Query 6: Approval rate trend (last 30 days)
-- SELECT 
--     scoring_date,
--     COUNT(*) as total_scored,
--     SUM(approved) as total_approved,
--     ROUND(100.0 * SUM(approved) / COUNT(*), 2) as approval_rate_pct,
--     ROUND(AVG(risk_score), 2) as avg_risk_score
-- FROM vinatien_analytics.ewa_eligibility_scores
-- WHERE scoring_date >= today() - 30
-- GROUP BY scoring_date
-- ORDER BY scoring_date DESC;

-- ================================================================
-- Retention Policy (Optional)
-- ================================================================

-- Keep detailed data for 1 year, then aggregate
-- ALTER TABLE vinatien_analytics.ewa_eligibility_scores 
-- MODIFY TTL scoring_date + INTERVAL 365 DAY;

-- ================================================================
-- Grants (Adjust based on your user roles)
-- ================================================================

-- Grant read access to analytics team
-- GRANT SELECT ON vinatien_analytics.ewa_eligibility_scores TO analytics_role;

-- Grant write access to ETL user
-- GRANT INSERT, SELECT ON vinatien_analytics.ewa_eligibility_scores TO airflow_user;
