# Deployment Guide — Fraud Detection & Compliance Pipeline

This document provides a step-by-step guide for deploying the Fraud Detection Pipeline locally and preparing it for a production environment.

---

## 1. Prerequisites

Ensure the following components are installed on your machine before proceeding:

- Python 3.12+
- Docker & Docker Desktop
- WSL 2 (Ubuntu) — required if running on Windows
- Confluent Cloud account (Kafka Broker)
- Snowflake account (Data Warehouse)

---

## 2. Environment Variables (`.env`)

Create a `.env` file in the root directory of the project. Ensure there are no spaces around the `=` sign.

```env
# Vault Configuration
VAULT_ADDR=http://localhost:8200
VAULT_TOKEN=root-token-123456

# Kafka / Confluent Configuration
KAFKA_BOOTSTRAP_SERVERS=your_bootstrap_server
KAFKA_API_KEY=your_api_key
KAFKA_API_SECRET=your_api_secret

# Snowflake Configuration (Fallback)
SNOWFLAKE_ACCOUNT=your_account
SNOWFLAKE_USER=your_user
SNOWFLAKE_PASSWORD=your_password
```

---

## 3. Deployment Steps

### Step 1 — Start Local Infrastructure (Docker)

Start all supporting services (Vault and local dependencies) using Docker Compose:

```bash
docker-compose up -d
```

### Step 2 — Configure Secret Management (HashiCorp Vault)

Register Snowflake credentials into Vault so the application can retrieve them securely at runtime:

```powershell
# PowerShell
$env:VAULT_ADDR="http://localhost:8200"
$env:VAULT_TOKEN="root-token-123456"

vault kv put secret/snowflake account="YOUR_ACC" user="YOUR_USER" password="YOUR_PASS"
```

### Step 3 — Prepare the Database (Snowflake)

Ensure the Medallion schema is in place by running the DDL scripts to create the following tables:

```
FRAUD_DB.BRONZE.bronze_raw_transactions
FRAUD_DB.SILVER.silver_enriched_transactions
FRAUD_DB.GOLD.gold_fraud_daily_summary
```

### Step 4 — Run the Data Pipeline

Install all Python dependencies:

```bash
pip install -r requirements.txt
```

Start the Consumer (Bronze Layer ingestion):

```bash
python consumer/consumer.py
```

Run the load test to simulate live traffic:

```bash
python scripts/load_test.py
```

---

## 4. Deployment Verification

The deployment is considered successful when all of the following conditions are met:

- Consumer logs display `Successfully connected to Snowflake`.
- Data flows into the Bronze table in Snowflake in real-time.
- Vault authenticates and returns secrets without errors.

---

## 5. Expected Performance Benchmarks

These benchmarks serve as the baseline for a healthy system under peak load:

| Metric | Expected Value |
|---|---|
| Throughput | ≥ 70 events/sec |
| Average Latency (end-to-end) | < 500 ms |
| Data Integrity (Bronze) | 100% |
| Success Rate | 100% |