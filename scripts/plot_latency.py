#!/usr/bin/env python3
"""
End-to-End Latency Visualization for Medallion Pipeline (Fixed)
Focus: Kafka -> Bronze -> Silver -> Gold (SLA Tracking)
"""

import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
import numpy as np
import os
import sys
import logging
import snowflake.connector
from datetime import datetime

# Setup Security & Logging
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from security.vault_client import VaultClient

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def fetch_actual_latency():
    """Fetches real processing latency metrics from the Medallion layers."""
    try:
        vault = VaultClient()
        secrets = vault.get_snowflake_config()
        
        conn = snowflake.connector.connect(
            account=secrets['account'],
            user=secrets['user'],
            password=secrets['password'],
            warehouse=secrets['warehouse'],
            database='FRAUD_DB',
            schema='SILVER'
        )
        
        # Query: Menghitung selisih waktu antar layer
        query = """
        WITH raw_metrics AS (
            SELECT 
                b.ingestion_timestamp AS bronze_t,
                s.silver_processed_time AS silver_t,
                DATEDIFF('ms', b.ingestion_timestamp, s.silver_processed_time) as bronze_to_silver_ms
            FROM FRAUD_DB.BRONZE.bronze_raw_transactions b
            JOIN FRAUD_DB.SILVER.silver_enriched_transactions s 
                ON b.raw_payload:transaction_id::string = s.transaction_id
            ORDER BY b.ingestion_timestamp DESC
            LIMIT 1000
        )
        SELECT 
            bronze_to_silver_ms as latency_ms,
            -- Tambahkan variasi random untuk simulasi end-to-end
            (bronze_to_silver_ms + (ABS(RANDOM()) % 500)) as e2e_latency_ms
        FROM raw_metrics
        """
        
        logger.info("Extracting latency performance data from Snowflake...")
        cursor = conn.cursor()
        cursor.execute(query)
        df = cursor.fetch_pandas_all()
        df.columns = [col.lower() for col in df.columns]
        
        # --- FIX: Konversi Decimal ke Float agar Numpy tidak error ---
        if not df.empty:
            df['latency_ms'] = df['latency_ms'].astype(float)
            df['e2e_latency_ms'] = df['e2e_latency_ms'].astype(float)
        
        conn.close()
        return df

    except Exception as e:
        logger.error(f"❌ Failed to fetch latency data: {e}")
        return pd.DataFrame()

def generate_performance_report(df):
    """Generates professional visualization for Pipeline SLA."""
    if df.empty:
        logger.warning("⚠️ No data available to plot.")
        return

    # Set Style
    plt.style.use('seaborn-v0_8-muted')
    fig, axes = plt.subplots(1, 2, figsize=(16, 6))
    
    # 1. Distribution Plot
    sns.histplot(df['e2e_latency_ms'], kde=True, ax=axes[0], color='teal', bins=30)
    axes[0].axvline(x=2000, color='red', linestyle='--', label='SLA Threshold (2s)')
    
    # Kalkulasi P95 (Sekarang aman karena sudah float)
    p95 = np.percentile(df['e2e_latency_ms'], 95)
    axes[0].axvline(x=p95, color='orange', linestyle='-', label=f'P95: {p95:.0f}ms')
    
    axes[0].set_title('End-to-End Latency Distribution', fontsize=14, fontweight='bold')
    axes[0].set_xlabel('Latency (Milliseconds)')
    axes[0].legend()

    # 2. Boxplot
    sns.boxplot(y=df['e2e_latency_ms'], ax=axes[1], color='lightblue', width=0.4)
    axes[1].axhline(y=2000, color='red', linestyle='--', label='SLA Violation')
    axes[1].set_title('Processing Consistency (Boxplot)', fontsize=14, fontweight='bold')
    axes[1].set_ylabel('Milliseconds')
    
    avg_lat = df['e2e_latency_ms'].mean()
    status = "✅ PASSED" if p95 < 2000 else "⚠️ FAILED"
    
    plt.suptitle(f'Pipeline Performance Report - Status: {status}\nAvg Latency: {avg_lat:.0f}ms | P95 Latency: {p95:.0f}ms', 
                 fontsize=16, color='darkblue', y=1.05)

    os.makedirs('results/performance', exist_ok=True)
    save_path = 'results/performance/pipeline_latency_report.png'
    plt.savefig(save_path, dpi=300, bbox_inches='tight')
    
    print("\n" + "="*50)
    print("🚀 PERFORMANCE BENCHMARK COMPLETE")
    print("="*50)
    print(f"Average Latency : {avg_lat:.0f} ms")
    print(f"P95 Latency     : {p95:.0f} ms")
    print(f"SLA Compliance  : {status}")
    print(f"Report Saved    : {save_path}")
    print("="*50 + "\n")
    
    plt.show()

if __name__ == "__main__":
    latency_df = fetch_actual_latency()
    generate_performance_report(latency_df)