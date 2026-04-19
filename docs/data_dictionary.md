# 📊 Data Dictionary - Fraud Detection Pipeline

## Transaction Attributes

| Field | Type | PII | Classification | Description | Example |
|-------|------|-----|----------------|-------------|---------|
| transaction_id | STRING | NO | Public | Unique transaction identifier | TXN-a1b2c3d4-e5f6 |
| user_id | STRING | NO (pseudonymized) | Internal | User identifier | USER_123456 |
| name | STRING | YES | HIGH - PII | Customer full name | John Doe |
| credit_card | STRING | YES | CRITICAL - PII | Credit card number | 4111111111111111 |
| amount | NUMBER | NO | Sensitive | Transaction amount (IDR) | 15000000 |
| location | STRING | NO | Public | Transaction city | Jakarta |
| timestamp | TIMESTAMP | NO | Public | Transaction time | 2024-01-15T10:30:00 |
| device_id | STRING | NO | Internal | Device identifier | device-abc123 |
| ip_address | STRING | YES | MEDIUM - PII | IP address | 192.168.1.1 |
| merchant | STRING | NO | Public | Merchant name | Tokopedia |
| category | STRING | NO | Public | Transaction category | ecommerce |
| raw_payload | VARIANT | NO | Internal | Complete raw JSON | {...} |

## PII Classification

| Level | Fields | Handling |
|-------|--------|----------|
| CRITICAL | credit_card | Mask to last 4 digits, encrypt at rest |
| HIGH | name | Show only initials |
| MEDIUM | ip_address | Mask network portion |
| LOW | user_id | Already pseudonymized |

## Data Quality Assumptions

1. **Completeness**: All required fields present
2. **Accuracy**: Amount > 0, timestamp not future
3. **Consistency**: Credit card passes Luhn check
4. **Timeliness**: Ingestion within 2 seconds
5. **Validity**: Location in predefined list

## Bronze Layer Schema (Raw Storage)

```sql
bronze_raw_transactions (
    raw_payload VARIANT,           -- Complete JSON
    ingestion_timestamp TIMESTAMP,
    source_partition INT,
    source_offset INT,
    data_quality_flags VARIANT
)