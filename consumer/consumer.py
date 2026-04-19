"""
Kafka Consumer for Fraud Detection Pipeline (Bronze Layer)
Consumes transactions from Confluent Cloud and performs batch ingestion to Snowflake.
Includes real-time data quality validation and latency monitoring.
"""
import sys
import json
import time
import logging
import os
import signal
from datetime import datetime
from typing import List, Dict, Any
from confluent_kafka import Consumer, KafkaError
import snowflake.connector
from dotenv import load_dotenv
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from security.vault_client import VaultClient

# Load environment variables
load_dotenv()

# Logging configuration
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class BronzeLayerConsumer:
    """Consumes Kafka messages and manages Snowflake ingestion logic"""

    def __init__(self):
        self.running = True
        self.batch_size = 100
        self.message_buffer = []
        self.stats = {'consumed': 0, 'errors': 0, 'latencies': []}

        # Inisialisasi Vault
        self.vault = VaultClient()
        secrets = self.vault.get_snowflake_config()
        
        # --- RETRY LOGIC UNTUK SNOWFLAKE ---
        self.snowflake_conn = None
        retries = 3
        while retries > 0:
            try:
                self.snowflake_conn = snowflake.connector.connect(
                    account=secrets['account'],
                    user=secrets['user'],
                    password=secrets['password'],
                    warehouse=secrets.get('warehouse', 'FRAUD_WH'), # Ambil dari vault atau default
                    database='FRAUD_DB',
                    schema='BRONZE'
                )
                logger.info("Successfully connected to Snowflake via Vault credentials")
                break 
            except Exception as e:
                logger.error(f"Snowflake connection failed: {e}")
                retries -= 1
                if retries == 0:
                    raise e 
                time.sleep(1)

        # Kafka Configuration (Gunakan env vars)
        kafka_conf = {
            'bootstrap.servers': os.getenv('CONFLUENT_BOOTSTRAP_SERVERS'),
            'sasl.mechanisms': 'PLAIN',
            'security.protocol': 'SASL_SSL',
            'sasl.username': os.getenv('CONFLUENT_API_KEY'),
            'sasl.password': os.getenv('CONFLUENT_API_SECRET'),
            'group.id': 'fraud-bronze-consumer-group-v5',
            'auto.offset.reset': 'earliest',
            'enable.auto.commit': True
        }
        
        self.consumer = Consumer(kafka_conf)
        self.consumer.subscribe(['fraud-transactions'])
        
        signal.signal(signal.SIGINT, self.signal_handler)
        logger.info("Bronze consumer initialized and subscribed to topic")

    def validate_data_quality(self, transaction: Dict) -> Dict[str, str]:
        """
        Performs basic data quality checks before ingestion.
        Returns a dictionary of found issues (flags).
        """
        flags = {}
        required_fields = ['transaction_id', 'user_id', 'amount', 'timestamp']
        
        # 1. Completeness Check
        for field in required_fields:
            if field not in transaction or transaction.get(field) is None:
                flags[field] = 'MISSING'

        # 2. Logic Check: Amount must be positive
        amount = transaction.get('amount', 0)
        if isinstance(amount, (int, float)) and amount <= 0:
            flags['amount'] = 'INVALID_VALUE_NON_POSITIVE'
        
        # 3. Timeliness Check: Timestamp should not be in the future
        try:
            txn_time = datetime.fromisoformat(transaction.get('timestamp', ''))
            if txn_time > datetime.now():
                flags['timestamp'] = 'FUTURE_DATE'
        except (ValueError, TypeError):
            flags['timestamp'] = 'INVALID_FORMAT'
            
        return flags

    def flush_to_snowflake(self):
        if not self.message_buffer:
            return

        logger.info(f"Submitting batch of {len(self.message_buffer)} records to Snowflake via Flatten Technique...")
        try:
            cursor = self.snowflake_conn.cursor()
            
            
            import json
            batch_data = [
                {
                    "p": r[0], # raw_payload (string)
                    "part": r[1], 
                    "off": r[2], 
                    "q": r[3]  # quality_flags (string)
                } for r in self.message_buffer
            ]
            json_payload = json.dumps(batch_data)

            sql = """
                INSERT INTO bronze_raw_transactions (
                    raw_payload, source_partition, source_offset, data_quality_flags
                )
                SELECT 
                    PARSE_JSON(f.value:p), 
                    f.value:part::INTEGER, 
                    f.value:off::INTEGER, 
                    PARSE_JSON(f.value:q)
                FROM TABLE(FLATTEN(INPUT => PARSE_JSON(%s))) f
            """
            
            cursor.execute(sql, (json_payload,))
            self.snowflake_conn.commit()
            cursor.close()
            
            logger.info(f"Successfully ingested {len(self.message_buffer)} records.")
            self.message_buffer = []
            
        except Exception as e:
            logger.error(f"Failed to write batch to Snowflake: {e}")
            self.message_buffer = [] 
            self.stats['errors'] += 1

    def calculate_latency(self, transaction: Dict) -> float:
        """Calculates end-to-end latency in milliseconds"""
        try:
            produce_time = datetime.fromisoformat(transaction.get('timestamp', ''))
            return (datetime.now() - produce_time).total_seconds() * 1000
        except:
            return 0.0

    def run(self, max_messages: int = None):
        """Main consumption loop with signal handling"""
        logger.info("Starting ingestion loop...")
        processed_count = 0
        
        try:
            while self.running:
                msg = self.consumer.poll(1.0)
                
                if msg is None:
                    continue
                if msg.error():
                    if msg.error().code() == KafkaError._PARTITION_EOF:
                        continue
                    else:
                        logger.error(f"Kafka error: {msg.error()}")
                        break

                try:
                    # Decode and parse message
                    val = msg.value().decode('utf-8')
                    transaction = json.loads(val)
                    
                    # Compute Metadata & Quality
                    quality_flags = self.validate_data_quality(transaction)
                    latency = self.calculate_latency(transaction)
                    
                    # Prepare data for batch insert
                    record = (
                        json.dumps(transaction),  # untuk raw_payload
                        int(msg.partition()),     # untuk source_partition
                        int(msg.offset()),        # untuk source_offset
                        json.dumps(quality_flags) # untuk data_quality_flags
                    )
                    
                    self.message_buffer.append(record)
                    self.stats['consumed'] += 1
                    if latency > 0:
                        self.stats['latencies'].append(latency)

                    # Trigger batch flush if size reached
                    if len(self.message_buffer) >= self.batch_size:
                        self.flush_to_snowflake()
                    
                    processed_count += 1
                    if processed_count % 100 == 0:
                        avg_lat = sum(self.stats['latencies']) / len(self.stats['latencies']) if self.stats['latencies'] else 0
                        logger.info(f"Progress: {processed_count} consumed | Avg Latency: {avg_lat:.0f}ms")

                    if max_messages and processed_count >= max_messages:
                        break

                except Exception as e:
                    logger.error(f"Error processing single message: {e}")
                    self.stats['errors'] += 1

            # Final flush for remaining messages in buffer
            self.flush_to_snowflake()

        finally:
            self.shutdown()

    def shutdown(self):
        """Graceful shutdown of consumer and database connection"""
        logger.info("Closing consumer and Snowflake connection...")
        self.consumer.close()
        self.snowflake_conn.close()
        logger.info(f"Finished. Total Records: {self.stats['consumed']} | Errors: {self.stats['errors']}")

    def signal_handler(self, sig, frame):
        """Interrupt signal handler"""
        logger.info("Shutdown signal received")
        self.running = False

if __name__ == "__main__":
    consumer = BronzeLayerConsumer()
    # You can set max_messages=2000 to catch up with all data in your topic
    consumer.run(max_messages=None)