"""
Kafka Producer for Fraud Detection Pipeline (Bronze Ingestion)
Generates realistic Indonesian transactions and sends them to Confluent Cloud.
This script is optimized for Medallion Architecture by sending clean JSON
which will be captured as a VARIANT in Snowflake's Bronze Layer.
"""

import json
import uuid
import time
import random
import signal
import os
import logging
from datetime import datetime
from typing import Dict, Any
from confluent_kafka import Producer
from faker import Faker
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Logging configuration
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Initialize Faker with Indonesian locale
fake = Faker('id_ID')

class FraudTransactionProducer:
    """Generates and sends transaction events to Kafka (Bronze layer)"""
    
    # Static business data based on the project scope
    LOCATIONS = ['Jakarta', 'Surabaya', 'Bandung', 'Medan', 'Semarang', 
                 'Denpasar', 'Makassar', 'Palembang', 'Batam', 'Manado']
    MERCHANTS = ['Tokopedia', 'Shopee', 'Bukalapak', 'Blibli', 'Lazada', 
                 'Alfamart', 'Indomaret', 'Hypermart', 'Gramedia', 'Traveloka']
    CATEGORIES = ['retail', 'food', 'travel', 'entertainment', 'utilities', 'ecommerce']
    
    def __init__(self):
        self.running = True
        self.stats = {'messages_sent': 0, 'errors': 0, 'start_time': None}
        
        # Confluent Cloud Configuration with Performance Tuning
        conf = {
            'bootstrap.servers': os.getenv('CONFLUENT_BOOTSTRAP_SERVERS'),
            'sasl.mechanisms': 'PLAIN',
            'security.protocol': 'SASL_SSL',
            'sasl.username': os.getenv('CONFLUENT_API_KEY'),
            'sasl.password': os.getenv('CONFLUENT_API_SECRET'),
            'compression.type': 'snappy',
            'acks': 'all',              # Ensure full propagation for fraud data
            'retries': 5,                # Increase reliability
            'linger.ms': 100,            # Wait 100ms to batch messages (Reduce latency/cost)
            'batch.size': 32768,         # 32KB batch size
            'client.id': 'fraud-producer-v1'
        }
        
        self.producer = Producer(conf)
        self.topic = 'fraud-transactions'
        
        # Handle graceful shutdown
        signal.signal(signal.SIGINT, self.signal_handler)
        logger.info("Kafka producer initialized for Bronze Ingestion")
    
    def generate_transaction(self) -> Dict[str, Any]:
        """
        Generate a single transaction with realistic IDR amounts.
        Follows the structure defined in docs/data_dictionary.md
        """
        now = datetime.now()
        is_fraud = random.random() < 0.05  # 5% probability of fraud
        
        # Logic: Fraudulent transactions often have higher amounts (IDR)
        if is_fraud:
            amount = round(random.uniform(10_000_000, 100_000_000), 2)
        else:
            amount = round(random.uniform(10_000, 5_000_000), 2)
        
        # Create transaction payload
        # Note: We do NOT create a nested raw_payload here to keep the message light.
        # The Snowflake Sink Connector will wrap this whole object into a VARIANT.
        return {
            "transaction_id": f"TXN-{uuid.uuid4().hex[:8].upper()}",
            "user_id": f"USER_{random.randint(1, 999999):06d}",
            "name": fake.name(),
            "credit_card": fake.credit_card_number(),
            "amount": amount,
            "location": random.choice(self.LOCATIONS),
            "timestamp": now.isoformat(),
            "device_id": str(uuid.uuid4()),
            "ip_address": fake.ipv4(),
            "merchant": random.choice(self.MERCHANTS),
            "category": random.choice(self.CATEGORIES),
            "is_fraud_simulated": is_fraud
        }
    
    def delivery_report(self, err, msg):
        """Callback called once per message to report delivery result"""
        if err:
            self.stats['errors'] += 1
            logger.error(f"Delivery failed for record {msg.key()}: {err}")
        else:
            self.stats['messages_sent'] += 1
    
    def send_transaction(self, transaction: Dict):
        """Asynchronously send transaction to Kafka"""
        key = transaction['user_id']
        value = json.dumps(transaction)
        
        try:
            self.producer.produce(
                topic=self.topic,
                key=key,
                value=value,
                callback=self.delivery_report
            )
            # Serve delivery reports from previous produce calls
            self.producer.poll(0)
        except BufferError:
            logger.warning("Local producer queue is full, waiting for deliveries...")
            self.producer.flush()
    
    def run(self, interval: float = 0.2, max_events: int = 1000):
        """Main loop to generate and send events"""
        self.stats['start_time'] = datetime.now()
        logger.info(f"Starting ingestion: {max_events} events at {interval}s interval")
        
        count = 0
        while self.running and (max_events is None or count < max_events):
            tx = self.generate_transaction()
            self.send_transaction(tx)
            
            count += 1
            if count % 100 == 0:
                logger.info(f"Progress: {count} messages sent to topic '{self.topic}'")
            
            time.sleep(interval)
        
        self.shutdown()

    def shutdown(self):
        """Clean up resources and log final stats"""
        logger.info("Flushing pending messages...")
        self.producer.flush()
        
        duration = (datetime.now() - self.stats['start_time']).total_seconds()
        logger.info("--- Production Summary ---")
        logger.info(f"Total Sent: {self.stats['messages_sent']}")
        logger.info(f"Total Errors: {self.stats['errors']}")
        logger.info(f"Execution Time: {duration:.2f} seconds")
        if duration > 0:
            logger.info(f"Throughput: {self.stats['messages_sent']/duration:.2f} msg/s")
        logger.info("--------------------------")

    def signal_handler(self, sig, frame):
        """Handle CTRL+C for graceful shutdown"""
        logger.info("Shutdown signal received (SIGINT)")
        self.running = False

if __name__ == "__main__":
    # Parameters: interval=0.2s for faster data generation, max_events=1000 for this batch
    producer = FraudTransactionProducer()
    producer.run(interval=0.1, max_events=1000)