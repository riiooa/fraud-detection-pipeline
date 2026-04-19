-- ============================================
-- BRONZE LAYER: Data Profiling & Quality Checks
-- ============================================

USE SCHEMA FRAUD_DB.BRONZE;

-- ============================================
-- 1. Basic Statistics
-- ============================================
SELECT 
    COUNT(*) as total_records,
    COUNT(DISTINCT raw_payload:transaction_id) as unique_transactions,
    COUNT(DISTINCT raw_payload:user_id) as unique_users,
    MIN(ingestion_timestamp) as first_ingestion,
    MAX(ingestion_timestamp) as last_ingestion
FROM bronze_raw_transactions;

-- ============================================
-- 2. Null Percentage Analysis
-- ============================================
SELECT 
    'transaction_id' as field,
    SUM(CASE WHEN raw_payload:transaction_id IS NULL THEN 1 ELSE 0 END) * 100.0 / COUNT(*) as null_pct
FROM bronze_raw_transactions
UNION ALL
SELECT 'user_id', SUM(CASE WHEN raw_payload:user_id IS NULL THEN 1 ELSE 0 END) * 100.0 / COUNT(*)
FROM bronze_raw_transactions
UNION ALL
SELECT 'amount', SUM(CASE WHEN raw_payload:amount IS NULL THEN 1 ELSE 0 END) * 100.0 / COUNT(*)
FROM bronze_raw_transactions
UNION ALL
SELECT 'location', SUM(CASE WHEN raw_payload:location IS NULL THEN 1 ELSE 0 END) * 100.0 / COUNT(*)
FROM bronze_raw_transactions;

-- ============================================
-- 3. Amount Distribution (Outlier Detection)
-- ============================================
SELECT 
    PERCENTILE_CONT(0.25) WITHIN GROUP (ORDER BY raw_payload:amount) as p25,
    PERCENTILE_CONT(0.50) WITHIN GROUP (ORDER BY raw_payload:amount) as median,
    PERCENTILE_CONT(0.75) WITHIN GROUP (ORDER BY raw_payload:amount) as p75,
    PERCENTILE_CONT(0.90) WITHIN GROUP (ORDER BY raw_payload:amount) as p90,
    PERCENTILE_CONT(0.95) WITHIN GROUP (ORDER BY raw_payload:amount) as p95,
    PERCENTILE_CONT(0.99) WITHIN GROUP (ORDER BY raw_payload:amount) as p99,
    AVG(raw_payload:amount) as avg_amount,
    STDDEV(raw_payload:amount) as stddev_amount,
    MIN(raw_payload:amount) as min_amount,
    MAX(raw_payload:amount) as max_amount
FROM bronze_raw_transactions;

-- ============================================
-- 4. Top Locations
-- ============================================
SELECT 
    raw_payload:location::STRING as location,
    COUNT(*) as transaction_count,
    SUM(raw_payload:amount) as total_amount,
    AVG(raw_payload:amount) as avg_amount
FROM bronze_raw_transactions
GROUP BY location
ORDER BY transaction_count DESC
LIMIT 10;

-- ============================================
-- 5. Data Quality Flags Summary
-- ============================================
SELECT 
    data_quality_flags:amount as amount_issue,
    data_quality_flags:timestamp as timestamp_issue,
    COUNT(*) as count
FROM bronze_raw_transactions
WHERE data_quality_flags IS NOT NULL
GROUP BY 1, 2;

-- ============================================
-- 6. Store Profiling Results (Optimized Version)
-- ============================================

INSERT INTO bronze_data_profile (
    profile_date,
    total_records,
    null_percentage,
    amount_stats,
    top_locations,
    quality_score
)
WITH base_stats AS (
    -- Tahap 1: Hitung statistik global & nulls
    SELECT 
        COUNT(*) as total_rec,
        OBJECT_CONSTRUCT(
            'transaction_id', SUM(CASE WHEN raw_payload:transaction_id IS NULL THEN 1 ELSE 0 END) * 100.0 / NULLIF(COUNT(*), 0),
            'user_id', SUM(CASE WHEN raw_payload:user_id IS NULL THEN 1 ELSE 0 END) * 100.0 / NULLIF(COUNT(*), 0),
            'amount', SUM(CASE WHEN raw_payload:amount IS NULL THEN 1 ELSE 0 END) * 100.0 / NULLIF(COUNT(*), 0)
        ) as null_pcts,
        OBJECT_CONSTRUCT(
            'min', MIN(raw_payload:amount::float),
            'max', MAX(raw_payload:amount::float),
            'avg', AVG(raw_payload:amount::float),
            'p95', PERCENTILE_CONT(0.95) WITHIN GROUP (ORDER BY raw_payload:amount::float)
        ) as amt_stats
    FROM bronze_raw_transactions
),
location_stats AS (
    -- Tahap 2: Hitung top locations secara terpisah
    SELECT 
        ARRAY_AGG(OBJECT_CONSTRUCT('location', loc, 'count', cnt)) 
            WITHIN GROUP (ORDER BY cnt DESC) as loc_array
    FROM (
        SELECT 
            raw_payload:location::STRING as loc, 
            COUNT(*) as cnt
        FROM bronze_raw_transactions
        GROUP BY 1
        ORDER BY cnt DESC
        LIMIT 10
    )
)
SELECT 
    CURRENT_DATE(),
    b.total_rec,
    b.null_pcts,
    b.amt_stats,
    l.loc_array,
    95.0 -- Quality Score
FROM base_stats b
CROSS JOIN location_stats l;
