-- ============================================
-- PDPA COMPLIANCE AUDIT QUERIES
-- Location: sql/audit_queries.sql
-- ============================================

USE ROLE ACCOUNTADMIN;

-- 1. Real-time PII Access Audit (Last 3 hours)
-- Menggunakan INFORMATION_SCHEMA untuk data paling update
SELECT 
    user_name,
    role_name,
    query_text,
    start_time,
    rows_produced,
    execution_status
FROM TABLE(INFORMATION_SCHEMA.QUERY_HISTORY(
    date_range_start => DATEADD('hour', -3, CURRENT_TIMESTAMP())
))
WHERE (query_text ILIKE '%credit_card%' 
    OR query_text ILIKE '%name%' 
    OR query_text ILIKE '%ip_address%')
ORDER BY start_time DESC;

-- 2. Security Test: Verify Masking as FRAUD_ANALYST
-- Pastikan role ini sudah di-setup dengan masking policy sebelumnya
USE ROLE FRAUD_ANALYST;
USE WAREHOUSE FRAUD_WH;
SELECT 
    name, 
    credit_card, 
    ip_address,
    location 
FROM FRAUD_DB.SILVER.silver_enriched_transactions 
LIMIT 10;

-- 3. Security Test: Verify Full Access as DATA_AUDITOR
USE ROLE DATA_AUDITOR;
SELECT 
    name, 
    credit_card, 
    ip_address 
FROM FRAUD_DB.SILVER.silver_enriched_transactions 
LIMIT 10;

-- 4. Compliance Summary View
CREATE OR REPLACE VIEW FRAUD_DB.SILVER.COMPLIANCE_AUDIT_REPORT AS
SELECT 
    DATE(start_time) as audit_date,
    role_name,
    COUNT(*) as total_sensitive_queries,
    COUNT(DISTINCT user_name) as unique_data_consumers
FROM TABLE(INFORMATION_SCHEMA.QUERY_HISTORY(
    date_range_start => DATEADD('day', -7, CURRENT_DATE())
))
WHERE query_text ILIKE '%FRAUD_DB%'
GROUP BY 1, 2;