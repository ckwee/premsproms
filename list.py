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


#########################################################################################################
#make sure you have done "az login" and set HTTPS_PROXY=http://proxy.health.qld.gov.au:8080 env variable
#########################################################################################################



        
def upload_file_to_datalake(container, target_folder, datalake_account_url):
    #check files existence on blob before delete... *outstanding*
    try:
        localfiles = glob.glob('c:/tmp/*.csv')
        default_credential = DefaultAzureCredential(exclude_shared_token_cache_credential=True)
        account_url ='https://adlehqcbiqhitfaedl.blob.core.windows.net/'
        blob_service_client = BlobServiceClient(datalake_account_url, credential=default_credential)
        container_client = blob_service_client.get_container_client(container)

        #looping files on local folder to upload 
        for f in localfiles:
            target_file=target_folder + os.path.basename(f)
            print("\nDeleting blob from Azure Storage:\n\t" + target_file)
            try:
                container_client.delete_blobs(target_file)
            except Exception as err:
                print("Encountered exception. {}".format(err))      
            #get relevant container and define blob target
            print("\nUploading to Azure Storage as blob:\n\t" + target_file)
            blob_client = blob_service_client.get_blob_client(container=container, blob=target_file)
            #loop and upload blobs to container
            with open(f, "rb") as data:
                blob_client.upload_blob(data)          
    except Exception as err:
        print("Encountered exception. {}".format(err))
                    

def keepass_get_password(myusername):   
    password=None
    try:
        kp = PyKeePass('KeePassDB.kdbx',password='wh0CanGuessThis1')
        entry=kp.find_entries(username=myusername, first=True)
        password=entry.password
    except Exception as err:
        print("Encountered exception. {}".format(err))
    return password

def azure_get_secret(secret_name):
    secret =None
    try:
        key_vault_url='https://kvtehqcbiqhitfaecbi.vault.azure.net/'
        credential = DefaultAzureCredential(exclude_shared_token_cache_credential=True)
        secret_client = SecretClient(vault_url=key_vault_url, credential=credential)
        secret = secret_client.get_secret(secret_name)
    except Exception as err:
        print("Encountered exception. {}".format(err))   
    return secret
	






def azure_move_blob_to_backup_folder(): # cp-r ./*.csv ./<new_folder>/
    #from datetime import datetime
    #backup_folder='backup' + datetime.now().strftime("%Y%m%d")+'/'
    #default_credential = DefaultAzureCredential(exclude_shared_token_cache_credential=True)
    #account_url ='https://adlehqcbiqhitfaedl.blob.core.windows.net/'
    #blob_service_client = BlobServiceClient(datalake_account_url, credential=default_credential)
    #container_client = blob_service_client.get_container_client(container)
    #blob_list = container_client.list_blobs()
    #for blob in blob_list:
    #    print("\t" + blob.name)
    pass
    

        
def list_file_on_datalake(container, target_folder, datalake_account_url):
    #check files existence on blob before delete... *outstanding*
    try:
        localfiles = glob.glob('c:/tmp/*.csv')
        default_credential = DefaultAzureCredential(exclude_shared_token_cache_credential=True)
        account_url ='https://adlehqcbiqhitfaedl.blob.core.windows.net/'
        blob_service_client = BlobServiceClient(datalake_account_url, credential=default_credential)
        container_client = blob_service_client.get_container_client(container)

        print("\nListing blobs...")
        # List the blobs in the container
        blob_list = container_client.list_blobs()
        for blob in blob_list:
            print("\t" + blob.name)

        
    except Exception as err:
        print("Encountered exception. {}".format(err))
                    


if __name__ == '__main__':
    config = configparser.ConfigParser()
    config.read("config.txt")
    ftp_download_dir    = config.get('FTP','ftp_download_dir')
    HOST                = config.get('FTP','HOST')
    USER                = config.get('FTP','USER')
    server              = config.get('AZURE','server')
    database            = config.get('AZURE','database')
    container           = config.get('AZURE','container')
    target_folder       = config.get('AZURE','target_folder')
    datalake_account_url= config.get('AZURE','datalake_account_url')
    kp_title            = config.get('keepass', 'title')
    kp_username         = config.get('keepass', 'username')
    PASS                = keepass_get_password(kp_username)           #from keepass
    
    print ('ftp_download_dir 	= ',ftp_download_dir) 
    print ('HOST		= ',HOST	 )
    print ('USER		= ',USER	 )	
    print ('PASS		= ',PASS	 )	
    print ('server 		= ',server 	 )	
    print ('database		= ',database	 )
    print ('container		= ',container	 )
    print ('target_folder	= ',target_folder)
    print ('datalake_account_url= ',datalake_account_url)
    print ('kp_title            = ',kp_title     )
    print ('kp_username         = ',kp_username  )
   
    ########### main job ############
    #remove_local_files(ftp_download_dir)
    #ftp_download_latest_zipfile(HOST, USER, PASS, ftp_download_dir)
    #upload_file_to_datalake(container, target_folder, datalake_account_url)
    list_file_on_datalake(container, target_folder, datalake_account_url)
    #exec_stored_proc_azure_db(server, database)

    print('******* done *******')

    
