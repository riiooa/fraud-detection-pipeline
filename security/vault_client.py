import os
import hvac
import logging
from functools import lru_cache

# Setup Logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class VaultClient:
    def __init__(self):
        # Gunakan env vars atau default
        self.vault_addr = os.getenv('VAULT_ADDR', 'http://localhost:8200')
        self.vault_token = os.getenv('VAULT_TOKEN', 'root-token-123456')
        
        try:
            self.client = hvac.Client(url=self.vault_addr, token=self.vault_token)
            if not self.client.is_authenticated():
                raise ConnectionError("Authentication failed")
            logger.info(f"Vault Connection Established: {self.vault_addr}")
        except Exception as e:
            logger.error(f"Vault Connection Error: {e}")
            raise

    @lru_cache(maxsize=32)
    def get_secrets(self, path: str):
        """Membaca semua secret di path tertentu"""
        try:
            # mount_point biasanya 'secret' untuk kv-v2 default
            response = self.client.secrets.kv.v2.read_secret_version(
                mount_point='secret', 
                path=path
            )
            return response['data']['data']
        except Exception as e:
            logger.error(f"Error reading secret at {path}: {e}")
            return None

    def get_confluent_config(self):
        data = self.get_secrets('confluent')
        # Format khusus untuk Confluent Kafka Consumer/Producer
        return {
            'bootstrap.servers': data.get('bootstrap_servers'),
            'sasl.username': data.get('api_key'),
            'sasl.password': data.get('api_secret'),
            'security.protocol': 'SASL_SSL',
            'sasl.mechanism': 'PLAIN'
        }

    def get_snowflake_config(self):
        return self.get_secrets('snowflake')

# Singleton instance
try:
    vault_client = VaultClient()
except:
    vault_client = None


if __name__ == "__main__":
    client = VaultClient()
    
    # Test ambil config Snowflake
    snowflake_conf = client.get_snowflake_config()
    print(f"Snowflake User: {snowflake_conf.get('user')}")
    
    # Test ambil config Confluent
    confluent_conf = client.get_confluent_config()
    print(f"Kafka Bootstrap: {confluent_conf.get('bootstrap.servers')}")