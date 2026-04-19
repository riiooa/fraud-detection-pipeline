# Troubleshooting Guide — Fraud Detection Pipeline

This document records common issues encountered during development and testing, along with their root causes and solutions.

---

## 1. `ModuleNotFoundError: No module named 'security'`

**Cause:** Python cannot locate the module folder because the script is being run from a subdirectory.

**Solution:** Add `sys.path` manipulation at the top of the script file:

```python
import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
```

---

## 2. `TypeError: 'NoneType' object is not subscriptable` (Vault)

**Cause:** The application failed to retrieve data from Vault because the secret has not been registered or the path is incorrect.

**Solution:**

1. Confirm the Vault server is running:
   ```bash
   docker ps
   ```
2. Verify that the Vault token is correct.
3. Re-register the secret to ensure it is stored properly:
   ```bash
   vault kv put secret/snowflake account="YOUR_ACC" user="YOUR_USER" password="YOUR_PASS"
   ```

---

## 3. `.env: line N: key cannot contain a space`

**Cause:** The `.env` file contains an invalid format — a space exists in the variable name or before the `=` sign.

**Solution:** Locate the line referenced in the error and correct the format. All variables must follow the `KEY=VALUE` pattern with no surrounding spaces.

```env
# Incorrect
SNOWFLAKE_USER = your_user

# Correct
SNOWFLAKE_USER=your_user
```

---

## 4. Low Success Rate (< 10%)

**Cause:** This is typically caused by one of the following:

- `batch_size` in `consumer.py` is too small relative to the ingestion frequency.
- The Snowflake Warehouse is in a `Suspended` or overloaded state.

**Solution:**

1. Increase `batch_size` in `consumer.py` to a minimum of `100`.
2. Confirm the Snowflake Warehouse is active (`Started`) before running the pipeline.

---

## 5. `Broker: Offset out of range`

**Cause:** A desync occurred between the offset stored by the Consumer Group and the data available in the Kafka topic. This commonly happens after a topic is deleted and recreated.

**Solution:** Update the `group.id` in the Kafka Consumer configuration to a new name (e.g., `fraud-consumer-v2`) to reset the read position:

```python
consumer_config = {
    "group.id": "fraud-consumer-v2",
    ...
}
```

---

## 6. Data Not Appearing in Silver Layer

**Cause:** Transformation failure or a connectivity issue between pipeline layers.

**Solution:**

1. Check the Consumer application logs for transformation errors.
2. Verify that the Silver Layer table schema matches the output of the Python transformation script.
3. Confirm the Snowflake `STREAM` and `TASK` are active and have not been suspended:
   ```sql
   SHOW TASKS IN SCHEMA FRAUD_DB.SILVER;
   ALTER TASK silver_enrichment_task RESUME;
   ```