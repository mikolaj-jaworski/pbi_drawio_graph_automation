import os
from azure.identity import ManagedIdentityCredential, DefaultAzureCredential
from azure.keyvault.secrets import SecretClient

def get_secret_value(secret_name: str) -> str:
    identity = ManagedIdentityCredential()
    secretClient = SecretClient(vault_url = os.environ['KeyVaultURL'], credential = identity)
    secret = secretClient.get_secret(secret_name)
    return secret.value