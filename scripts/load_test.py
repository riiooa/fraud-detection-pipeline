#!/usr/bin/env python3
"""
Load Testing for Medallion Pipeline
Pro-level monitoring of Bronze and Silver layers during high-velocity ingestion.
"""

import time
import json
import threading
import os
import sys
from datetime import datetime
import snowflake.connector

# Ensure root directory is in path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from producer.producer import FraudTransactionProducer
from security.vault_client import VaultClient

class LoadTester:
    def __init__(self, num_events=10000):
        self.num_events = num_events
        self.producer = FraudTransactionProducer()
        self.is_running = True
        
        # Metadata storage
        self.results = {
            'start_time': None,
            'end_time': None,
            'events_sent': 0,
            'bronze_count': 0,
            'silver_count': 0
        }

    def _get_snowflake_connection(self):
        """Ambil kredensial dari Vault secara aman."""
        vault = VaultClient()
        secrets = vault.get_snowflake_config()
        return snowflake.connector.connect(
            account=secrets['account'],
            user=secrets['user'],
            password=secrets['password'],
            warehouse=secrets.get('warehouse', 'FRAUD_WH'),
            database='FRAUD_DB'
        )

    def monitor_snowflake(self):
        """Thread function: Memantau pertumbuhan data di Snowflake secara berkala."""
        try:
            conn = self._get_snowflake_connection()
            while self.is_running:
                cursor = conn.cursor()
                try:
                    # Query Bronze Count
                    cursor.execute("SELECT COUNT(*) FROM BRONZE.bronze_raw_transactions")
                    self.results['bronze_count'] = cursor.fetchone()[0]
                    
                    # Query Silver Count
                    cursor.execute("SELECT COUNT(*) FROM SILVER.silver_enriched_transactions")
                    self.results['silver_count'] = cursor.fetchone()[0]
                except Exception as e:
                    print(f"\n[Monitor Error] {e}")
                finally:
                    cursor.close()
                
                time.sleep(3) # Cek setiap 3 detik agar tidak membebani Warehouse
            conn.close()
        except Exception as e:
            print(f"Failed to start Snowflake monitor: {e}")

    def run(self):
        print(f"🚀 INITIALIZING LOAD TEST: {self.num_events:,} EVENTS")
        print(f"Target Throughput: ~100 events/sec")
        print("-" * 50)
        
        self.results['start_time'] = time.time()
        
        # Jalankan Monitoring Thread
        monitor_thread = threading.Thread(target=self.monitor_snowflake, daemon=True)
        monitor_thread.start()
        
        try:
            # Pengiriman Event
            for i in range(self.num_events):
                # Gunakan data generator dari producer
                transaction = self.producer.generate_transaction()
                self.producer.send_transaction(transaction)
                self.results['events_sent'] += 1
                
                if (i + 1) % 1000 == 0:
                    now = datetime.now().strftime("%H:%M:%S")
                    print(f"[{now}] 📊 Progress: {i+1:,}/{self.num_events:,}")
                    print(f"      Status -> Bronze: {self.results['bronze_count']:,} | Silver: {self.results['silver_count']:,}")
                
                # Pace control: menghindari overloading Kafka local (jika perlu)
                time.sleep(0.01) 
                
        except KeyboardInterrupt:
            print("\n⚠️ Test dihentikan oleh user.")
        
        # Tunggu buffer Kafka & Snowflake Ingestion selesai
        print("\n⏳ Menunggu sinkronisasi akhir (30s)...")
        time.sleep(30)
        
        self.is_running = False
        self.results['end_time'] = time.time()
        monitor_thread.join(timeout=5)
        
        self.generate_report()

    def generate_report(self):
        duration = self.results['end_time'] - self.results['start_time']
        throughput = self.results['events_sent'] / duration
        
        # Hitung success rate berdasarkan data yang sampai ke Silver
        success_rate = (self.results['silver_count'] / self.num_events) * 100 if self.num_events > 0 else 0
        
        report = {
            'test_timestamp': datetime.now().isoformat(),
            'total_events_target': self.num_events,
            'total_events_sent': self.results['events_sent'],
            'duration_seconds': round(duration, 2),
            'throughput_per_sec': round(throughput, 2),
            'bronze_final_count': self.results['bronze_count'],
            'silver_final_count': self.results['silver_count'],
            'ingestion_success_rate': f"{success_rate:.2f}%"
        }

        # Simpan Report
        output_dir = 'results'
        if not os.path.exists(output_dir):
            os.makedirs(output_dir)
            
        file_path = os.path.join(output_dir, 'load_test_report.json')
        with open(file_path, 'w') as f:
            json.dump(report, f, indent=4)

        print("\n" + "="*60)
        print("LOAD TEST COMPLETED")
        print("="*60)
        print(f"⏱ Duration      : {report['duration_seconds']}s")
        print(f"Throughput    : {report['throughput_per_sec']} events/sec")
        print(f"Bronze Status : {report['bronze_final_count']:,} records")
        print(f"Silver Status : {report['silver_final_count']:,} records")
        print(f"Success Rate  : {report['ingestion_success_rate']}")
        print(f"📄Report Saved  : {file_path}")
        print("="*60)

if __name__ == "__main__":
    # Menjalankan 10k event
    tester = LoadTester(num_events=10000)
    tester.run()