import pytest
import sys
import os
from unittest.mock import patch

# Menambahkan root directory ke sys.path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

class TestSilverLayer:
    
    @pytest.fixture
    def sample_silver_row(self):
        """Fixture data yang merepresentasikan record di Silver Layer."""
        return {
            "transaction_id": "TXN-test123",
            "user_id": "USER_123456",
            "amount": 15000000,
            "location": "Jakarta",
            "is_fraud": True,
            "risk_score": 70
        }
    
    def test_fraud_detection_high_amount(self):
        """Test rule fraud: Transaksi > 50jt otomatis ditandai fraud."""
        from scripts.process_silver import FraudEngine # Asumsi nama file logic kamu
        
        amount = 60000000
        is_fraud, _ = FraudEngine.analyze_transaction(amount, "Jakarta")
        assert is_fraud == True
    
    def test_risk_score_calculation(self):
        """Memastikan perhitungan risk score sesuai dengan tier nominal transaksi."""
        from scripts.process_silver import FraudEngine
        
        # Test Case Mapping: (Amount, Expected Score)
        test_cases = [
            (60000000, 95),
            (20000000, 70),
            (8000000, 40),
            (1000000, 10)
        ]
        
        for amount, expected_score in test_cases:
            _, score = FraudEngine.analyze_transaction(amount, "Any")
            assert score == expected_score, f"Amount {amount} harusnya score {expected_score}"
    
    def test_location_risk_level(self):
        """Memastikan lokasi berisiko tinggi mendapatkan flag yang benar."""
        from scripts.process_silver import FraudEngine
        
        assert FraudEngine.get_location_risk('Jakarta') == 'HIGH'
        assert FraudEngine.get_location_risk('Bandung') == 'MEDIUM'
        assert FraudEngine.get_location_risk('Denpasar') == 'LOW'

    def test_pii_masking_consistency(self):
        """Memastikan fungsi masking tidak merusak format data."""
        from scripts.process_silver import DataPrivacy
        
        card = "1234-5678-9012-3456"
        masked = DataPrivacy.mask_credit_card(card)
        assert masked == "****-****-****-3456"
        assert len(masked) == len(card)