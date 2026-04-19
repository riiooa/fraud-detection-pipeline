-- create a role
CREATE ROLE IF NOT EXISTS FRAUD_ANALYST;
CREATE ROLE IF NOT EXISTS DATA_AUDITOR;

-- Grant schema access to the role
GRANT USAGE ON DATABASE FRAUD_DB TO ROLE FRAUD_ANALYST;
GRANT USAGE ON SCHEMA FRAUD_DB.SILVER TO ROLE FRAUD_ANALYST;
GRANT SELECT ON TABLE FRAUD_DB.SILVER.silver_enriched_transactions TO ROLE FRAUD_ANALYST;

GRANT USAGE ON DATABASE FRAUD_DB TO ROLE DATA_AUDITOR;
GRANT USAGE ON SCHEMA FRAUD_DB.SILVER TO ROLE DATA_AUDITOR;
GRANT SELECT ON TABLE FRAUD_DB.SILVER.silver_enriched_transactions TO ROLE DATA_AUDITOR;

-- ============================================
-- DYNAMIC DATA MASKING for PDPA Compliance
-- Silver Layer PII Protection
-- ============================================

USE SCHEMA FRAUD_DB.SILVER;

-- ============================================
-- 1. Credit Card Masking (Last 4 digits only)
-- ============================================
CREATE OR REPLACE MASKING POLICY mask_credit_card AS (val STRING) RETURNS STRING ->
    CASE
        WHEN CURRENT_ROLE() IN ('DATA_AUDITOR', 'ACCOUNTADMIN') THEN val
        ELSE CONCAT('****-****-****-', RIGHT(val, 4))
    END;

-- ============================================
-- 2. Name Masking (Initials only)
-- ============================================
CREATE OR REPLACE MASKING POLICY mask_name AS (val STRING) RETURNS STRING ->
    CASE
        WHEN CURRENT_ROLE() IN ('DATA_AUDITOR', 'ACCOUNTADMIN') THEN val
        ELSE CONCAT(LEFT(val, 1), '****', RIGHT(val, 1))
    END;

-- ============================================
-- 3. IP Address Masking (Network part only)
-- ============================================
CREATE OR REPLACE MASKING POLICY mask_ip AS (val STRING) RETURNS STRING ->
    CASE
        WHEN CURRENT_ROLE() IN ('DATA_AUDITOR', 'ACCOUNTADMIN') THEN val
        ELSE CONCAT(SPLIT_PART(val, '.', 1), '.***.***.***')
    END;

-- ============================================
-- 4. Apply Masking Policies to Silver Table
-- ============================================
ALTER TABLE silver_enriched_transactions 
    MODIFY COLUMN credit_card SET MASKING POLICY mask_credit_card;

ALTER TABLE silver_enriched_transactions 
    MODIFY COLUMN name SET MASKING POLICY mask_name;

ALTER TABLE silver_enriched_transactions 
    MODIFY COLUMN ip_address SET MASKING POLICY mask_ip;
    