import pytest
import sys
import os
from unittest.mock import Mock, patch
from datetime import datetime, timedelta

# Menambahkan root directory ke sys.path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

class TestBronzeLayer:
    
    @pytest.fixture
    def sample_transaction(self):
        """Fixture untuk menyediakan data transaksi dummy yang valid."""
        return {
            "transaction_id": "TXN-test123",
            "user_id": "USER_123456",
            "amount": 15000000,
            "timestamp": "2024-01-15T10:30:00",
            "location": "Jakarta"
        }
    
    @patch('snowflake.connector.connect')
    def test_data_quality_validation(self, mock_connect, sample_transaction):
        """Memastikan data valid tidak menghasilkan error flags."""
        from consumer.consumer import BronzeLayerConsumer
        # Mocking koneksi agar tidak benar-benar konek ke Snowflake saat init
        consumer = BronzeLayerConsumer()
        
        flags = consumer.validate_data_quality(sample_transaction)
        assert len(flags) == 0, f"Harusnya 0 flags, tapi ditemukan: {flags}"
    
    @patch('snowflake.connector.connect')
    def test_invalid_amount_negative(self, mock_connect, sample_transaction):
        """Validasi jika amount bernilai negatif (Anomaly)."""
        from consumer.consumer import BronzeLayerConsumer
        consumer = BronzeLayerConsumer()
        
        sample_transaction['amount'] = -1000
        flags = consumer.validate_data_quality(sample_transaction)
        assert 'amount' in flags, "Gagal mendeteksi amount negatif"
    
    @patch('snowflake.connector.connect')
    def test_missing_required_field(self, mock_connect, sample_transaction):
        """Validasi jika field wajib hilang dari payload."""
        from consumer.consumer import BronzeLayerConsumer
        consumer = BronzeLayerConsumer()
        
        del sample_transaction['transaction_id']
        flags = consumer.validate_data_quality(sample_transaction)
        assert 'transaction_id' in flags, "Gagal mendeteksi missing field: transaction_id"
    
    @patch('snowflake.connector.connect')
    def test_future_timestamp(self, mock_connect, sample_transaction):
        """Validasi jika timestamp transaksi berada di masa depan."""
        from consumer.consumer import BronzeLayerConsumer
        consumer = BronzeLayerConsumer()
        
        # Set ke besok
        future_date = (datetime.now() + timedelta(days=1)).isoformat()
        sample_transaction['timestamp'] = future_date
        
        flags = consumer.validate_data_quality(sample_transaction)
        assert 'timestamp' in flags, "Gagal mendeteksi future timestamp"

    @patch('confluent_kafka.Producer')
    def test_retry_logic_on_kafka_failure(self, mock_producer):
        """Memastikan producer melempar exception saat koneksi Kafka gagal."""
        mock_producer.side_effect = Exception("Kafka connection failed")
        
        from producer.producer import FraudTransactionProducer
        with pytest.raises(Exception) as excinfo:
            FraudTransactionProducer()
        assert "Kafka connection failed" in str(excinfo.value)

    @patch('snowflake.connector.connect')
    def test_snowflake_connection_retry(self, mock_connect):
        """
        Simulasi Error pada percobaan pertama, dan sukses pada percobaan kedua.
        Menguji ketahanan koneksi database.
        """
        mock_connect.side_effect = [Exception("Timeout"), Mock()]
        
        from consumer.consumer import BronzeLayerConsumer
        # Inisialisasi harus tetap berhasil karena adanya retry logic internal
        consumer = BronzeLayerConsumer()
        assert consumer.snowflake_conn is not None
        assert mock_connect.call_count == 2