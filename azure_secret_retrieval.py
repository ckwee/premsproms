from azure.identity import DefaultAzureCredential,AzureCliCredential
from azure.keyvault.secrets import SecretClient


def get_secret(secret_name):
	key_vault_url='https://kvtehqcbiqhitfaecbi.vault.azure.net/'
	credential = DefaultAzureCredential(exclude_shared_token_cache_credential=True)
	secret_client = SecretClient(vault_url=key_vault_url, credential=credential)
	secret = secret_client.get_secret(secret_name)
	return(secret)
	
	

a=get_secret('IEMR-CERT0')
print(a.name,'         ', a.value)
	
