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
from pykeepass import PyKeePass


#########################################################################################################
#make sure you have done "az login" and set HTTPS_PROXY=http://proxy.health.qld.gov.au:8080 env variable
#########################################################################################################

def remove_local_files(ftp_download_dir):
    #removing all the local files on the local drive
    
    localfiles = glob.glob(ftp_download_dir + '*.csv', recursive=True)
    print(ftp_download_dir+ '*.csv')
    for f in localfiles:
        print('Deleting files ... ' + f) 
        try:
            os.remove(f)
        except OSError as e:
            print("Error: %s : %s" % (f, e.strerror))  


def ftp_download_all_files(HOST, USER, PASS, ftp_download_dir):
    #download all the files from the ftp server regardless of types and dates
    try:
        ftp=ftplib.FTP(HOST,USER,PASS)
        for file in ftp.nlst():
            ftp.retrbinary("RETR " + file , open(ftp_download_dir + file, 'wb').write)
            print('Downloading files to ' + ftp_download_dir + ' .... ' + file) 
        ftp.close()
    except ftplib.all_errors as e: 
        print(e)


def ftp_download_latest_zipfile(HOST, USER, PASS, ftp_download_dir):
    #download only the latest zipped file from the ftp server
    try:
        ftp=ftplib.FTP(HOST,USER,PASS)
        ftp_files_list = ftp.nlst('*.zip')
        latest_time = None
        latest_name = None
        #find latest zip files available in ftp dir
        for ftp_file in ftp_files_list:          
            time = ftp.sendcmd("MDTM " + ftp_file)
            if (latest_time is None) or (time > latest_time):
                latest_file = ftp_file
                latest_time = time
        #loop and download files
        print('Downloading latest zip file to ' + ftp_download_dir + '..........' + latest_file)
        file = open(latest_file, 'wb')
        ftp.retrbinary('RETR '+ latest_file, open(ftp_download_dir + latest_file, 'wb').write)
        #unzipping downloaded zipped file
        zf = ZipFile(ftp_download_dir + latest_file, 'r')
        zf.extractall(ftp_download_dir)
        zf.close()
        #close ftp 
        ftp.close()
    except ftplib.all_errors as e: 
        print(e)
        
        
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
            #print("\nDeleting blob from Azure Storage:\n\t" + target_file)
            try:
                ##############
                blob_container_client = container_client.get_blob_client(target_file)
                if blob_container_client.exists():
                    print ('\ntarget file '+ target_file+' exists, deleting them ....')
                #############
                    container_client.delete_blobs(target_file)
                else:
                    print(target_file + ' does not exist...')
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
                    

def exec_stored_proc_azure_db(server, database):  
    driver = '{ODBC Driver 17 for SQL Server}'
    try:
        credential = DefaultAzureCredential(exclude_shared_token_cache_credential=True)
        token = credential.get_token('https://database.windows.net/').token.encode('utf-16-le')
        token_struct = struct.pack(f'<I{len(token)}s', len(token), token)
        conn = pyodbc.connect('DRIVER=' + driver + ';SERVER=' + server + ';PORT=1433; DATABASE=' + database, attrs_before={1256:token_struct},autocommit = True)
        sqlSP = "EXEC ODS_QUESTLINK.[usp_QUESTLINK_LOAD_DL_SYN]'https://adlehqcbiqhitfaedl.dfs.core.windows.net/raw/landing/prems_proms/'"
        #conn.execute(sqlSP)

        ##########################
        cursor = conn.cursor()
        cursor.execute(sqlSP)     
        row = cursor.fetchone()
        while row:
            print(row)
            row = cursor.fetchone()
        cursor.close()
        ##########################

        conn.close()
    except pyodbc.ProgrammingError as err:
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
    remove_local_files(ftp_download_dir)
    ftp_download_latest_zipfile(HOST, USER, PASS, ftp_download_dir)
    upload_file_to_datalake(container, target_folder, datalake_account_url)
    exec_stored_proc_azure_db(server, database)

    print('******* done *******')

    
