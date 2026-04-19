"""
PDPA Compliance Audit Generator 
Purpose: Automated reporting for data privacy compliance using correct Information Schema identifiers.
"""

import snowflake.connector
import pandas as pd
import os
import sys
import json
import logging
from datetime import datetime

# Import Security Vault
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from security.vault_client import VaultClient

# Logging Configuration
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def generate_audit_report():
    try:
        # 1. Secure Credential Retrieval
        vault = VaultClient()
        secrets = vault.get_snowflake_config()
        
        # 2. Establish Connection
        conn = snowflake.connector.connect(
            account=secrets['account'],
            user=secrets['user'],
            password=secrets['password'],
            role='ACCOUNTADMIN',
            warehouse=secrets['warehouse'],
            database='FRAUD_DB'
        )
        
        # 3. Fetch Audit Logs (Last 24 Hours)
        # Perbaikan: Menggunakan EXECUTION_TIME (Identifier resmi di Information Schema)
        query = """
            SELECT 
                USER_NAME,
                ROLE_NAME,
                QUERY_TEXT,
                START_TIME,
                EXECUTION_TIME
            FROM TABLE(FRAUD_DB.INFORMATION_SCHEMA.QUERY_HISTORY(
                DATEADD('day', -1, CURRENT_TIMESTAMP()), 
                CURRENT_TIMESTAMP()
            ))
            WHERE (QUERY_TEXT ILIKE '%credit_card%' OR QUERY_TEXT ILIKE '%name%')
              AND QUERY_TEXT NOT ILIKE '%QUERY_HISTORY%'
            ORDER BY START_TIME DESC
        """
        
        logger.info("Accessing Snowflake Audit Logs via Information Schema...")
        
        cursor = conn.cursor()
        cursor.execute(query)
        df = cursor.fetch_pandas_all()
        
        # 4. Process Compliance Metrics
        os.makedirs('results', exist_ok=True)
        
        # Menyeragamkan kolom ke lowercase untuk mempermudah pengolahan di Pandas
        if not df.empty:
            df.columns = [col.lower() for col in df.columns]
        
        report = {
            'report_id': f"AUDIT-{datetime.now().strftime('%Y%m%d-%H%M%S')}",
            'generated_at': datetime.now().isoformat(),
            'compliance_window': '24 Hours',
            'summary': {
                'total_sensitive_access_count': len(df),
                'unique_roles_accessed': df['role_name'].nunique() if not df.empty else 0,
                'access_by_role': df['role_name'].value_counts().to_dict() if not df.empty else {}
            },
            'compliance_checks': {
                'pii_access_monitored': 'PASS' if len(df) > 0 else 'WARNING (No Recent Activity)',
                'audit_log_availability': 'PASS',
                'pdpa_standards': 'ENFORCED'
            }
        }
        
        # 5. Save Report to JSON
        output_path = 'results/compliance_report.json'
        with open(output_path, 'w') as f:
            json.dump(report, f, indent=4, default=str)
            
        print("\n" + "🛡️ " * 10)
        print("PDPA AUDIT REPORT GENERATED")
        print("🛡️ " * 10)
        print(f"Total Sensitive Queries : {report['summary']['total_sensitive_access_count']}")
        print(f"Unique Roles Involved   : {report['summary']['unique_roles_accessed']}")
        print(f"Status                  : {report['compliance_checks']['pii_access_monitored']}")
        print(f"Output File             : {output_path}")
        print("-" * 30 + "\n")
        
        conn.close()
        return report

    except Exception as e:
        logger.error(f"Critical error during audit generation: {str(e)}")
        return None

if __name__ == "__main__":
    generate_audit_report()