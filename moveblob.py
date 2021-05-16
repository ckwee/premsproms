#!az login
#! set HTTPS_PROXY=http://proxy.health.qld.gov.au:8080

from azure.identity import DefaultAzureCredential
from azure.storage.blob import BlobServiceClient
from azure.identity import DefaultAzureCredential,AzureCliCredential
from azure.keyvault.secrets import SecretClient
import ftplib 
import os
import glob
import struct
import pyodbc
from zipfile import ZipFile
import configparser         


from datetime import datetime
container='test'
backup_folder='/backup' + datetime.now().strftime("%Y%m%d")+'/'
default_credential = DefaultAzureCredential(exclude_shared_token_cache_credential=True)
datalake_account_url ='https://adlehqcbiqhitfaedl.blob.core.windows.net/'
blob_service_client = BlobServiceClient(datalake_account_url, credential=default_credential)
container_client = blob_service_client.get_container_client(container)
blob_list = container_client.list_blobs()

source_container_name=container + '/'
target_container_name=container + backup_folder
for blob in blob_list:
    source_blob = datalake_account_url + source_container_name + blob.name
    target_blob = datalake_account_url + target_container_name + blob.name
    print(source_blob)
    print(target_blob)
    copy_target_blob = blob_service_client(datalake_account_url + target_container_name, blob.name)
    copy_target_blob.start_copy_from_url(source_blob)
    
    
