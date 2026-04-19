class FraudEngine:
    @staticmethod
    def analyze_transaction(amount, location):
        # Rule 1: Amount Threshold
        is_fraud = amount > 50000000
        
        # Rule 2: Risk Scoring
        if amount > 50000000:
            score = 95
        elif amount > 10000000:
            score = 70
        elif amount > 5000000:
            score = 40
        else:
            score = 10
            
        return is_fraud, score

    @staticmethod
    def get_location_risk(location):
        high_risk = ['Jakarta', 'Surabaya']
        medium_risk = ['Bandung', 'Medan']
        if location in high_risk:
            return 'HIGH'
        elif location in medium_risk:
            return 'MEDIUM'
        return 'LOW'

class DataPrivacy:
    @staticmethod
    def mask_credit_card(card_number):
        if not card_number or len(card_number) < 4:
            return "INVALID"
        return f"****-****-****-{card_number[-4:]}"