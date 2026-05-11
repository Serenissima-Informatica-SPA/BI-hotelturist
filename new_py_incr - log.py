import ssl
from requests_ntlm import HttpNtlmAuth
import requests
import hashlib
from Crypto.Hash import MD4
import pandas as pd
from datetime import datetime, timedelta
from concurrent.futures import ThreadPoolExecutor, as_completed
from azure.storage.blob import BlobServiceClient
from io import BytesIO
from azure.core.exceptions import ResourceExistsError
import calendar
import uuid
import os
import json
import io
import time
import pyarrow as pa
import fastparquet
import sys
from cryptography.fernet import Fernet

import logging 
import smtplib 


# PER VERIFICARE FUNZIONAMENTO AGGIORNAMENTO

# # Funzione per generare un nuovo valore "SystemId"
# def generate_system_id():
#     return str(uuid.uuid4())

# # Leggi il file Parquet
# file_path = "//serinf-navdev/App AL/Power BI Data Source/ONPREM/LDF/ValueEntryQry_2024_02(2).parquet"
# df = pd.read_parquet(file_path)

# # Seleziona le ultime 5 righe
# last_five_rows = df.tail(5).copy()

# # Modifica il campo "SystemId" con nuovi valori
# last_five_rows['SystemId'] = [generate_system_id() for _ in range(5)]

# # Aggiungi le righe modificate al DataFrame originale
# prova = pd.concat([df, last_five_rows], ignore_index=True)

# # Salva il DataFrame aggiornato su un nuovo file Parquet (o sovrascrivi il file esistente)
# updated_file_path = "//serinf-navdev/App AL/Power BI Data Source/ONPREM/LDF/ValueEntryQry_2024_02.parquet"  # Modifica se necessario
# prova.to_parquet(updated_file_path)

# print(f"File aggiornato salvato in: {updated_file_path}")


# import pandas as pd
# import uuid
# from datetime import datetime, timedelta

# def generate_system_id():
#     return str(uuid.uuid4())

# # Leggi il file Parquet
# file_path = "//serinf-navdev/App AL/Power BI Data Source/ONPREM/LDF/ValueEntryQry_2024_02(2).parquet"
# df = pd.read_parquet(file_path)

# # Seleziona l'ultima riga
# last_row = df.tail(1).copy()
# last_row['SystemModifiedAt'] = "2024-06-20T07:35:46.917Z"

# # Aggiorna l'ultima riga del DataFrame originale
# df.iloc[-1] = last_row.iloc[0]

# # Salva il DataFrame aggiornato su un nuovo file Parquet (o sovrascrivi il file esistente)
# updated_file_path = "//serinf-navdev/App AL/Power BI Data Source/ONPREM/LDF/ValueEntryQry_2024_02.parquet"  # Modifica se necessario
# df.to_parquet(updated_file_path)

#####################
# LETTURA FILE JSON #
#####################

# Ottieni la directory in cui si trova l'eseguibile
if getattr(sys, 'frozen', False):
    # Se il codice è eseguito come eseguibile
    base_path = os.path.dirname(sys.executable)
else:
    # Se il codice è eseguito come script Python
    base_path = os.path.dirname(os.path.abspath(__file__))

# Costruisci il percorso al file di configurazione
config_file_path = os.path.join(base_path, 'configuration.json')

# Ora puoi usare config_file_path per leggere il tuo file di configurazione
print(f"Percorso del file di configurazione: {config_file_path}")

# Esempio di lettura del file di configurazione
import json
with open(config_file_path, 'r') as f:
    config = json.load(f)
    # print(config)

# PER DECRIPTARE LE CREDENZIALI



# # Funzione per caricare la chiave
# def load_key():
#     return open('secret.key', 'rb').read()

# # Funzione per decriptare un messaggio
# def decrypt_message(encrypted_message):
#     key = load_key()
#     f = Fernet(key)
#     decrypted_message = f.decrypt(encrypted_message.encode())
#     return decrypted_message.decode()

# # Carica le credenziali criptate da un file JSON
# with open('configuration.json', 'r') as json_file:
#     encrypted_credentials = json.load(json_file)

# # Decripta le credenziali
# decrypted_credentials = {key: decrypt_message(value) for key, value in encrypted_credentials.items()}

# print("Credenziali decriptate:", decrypted_credentials)

# # Utilizza le credenziali decriptate
# username = decrypted_credentials["username1"]
# password = decrypted_credentials["password1"]

# Ad esempio, si può utilizzare per una connessione
# connessione(username, password)

# Ottieni il percorso della directory in cui si trova lo script
# current_directory = os.path.dirname(os.path.abspath(__file__))

# # Specifica il nome del file JSON
# json_filename = 'configuration.json'

# # Combina il percorso della directory con il nome del file JSON
# json_filepath = os.path.join(current_directory, json_filename)

# # Leggi il file JSON
# try:
#     with open(json_filepath, 'r', encoding='utf-8') as file:
#         config = json.load(file)
#         # print(config)
# except FileNotFoundError:
#     print(f"File JSON non trovato: {json_filepath}")
# except json.JSONDecodeError:
#     print(f"Errore nel decodificare il file JSON: {json_filepath}")

# LETTURA PARAMETRI

connection_string = config.get('connection_string', '')
blob_service_client = BlobServiceClient.from_connection_string(connection_string)

BC_type = config.get('BC_type', '')
first_time = config.get('first_time', '')
cust = config.get('cust', '')

IP = config.get('IP', '')
Port = config.get('Port_onprem', '')
ServiceName = config.get('ServiceName', '')
username = config.get('username', '')
password = config.get('password', '')

# Parametri comuni
start_year = config.get('start_year')
end_year = datetime.utcnow().year
current_year = config.get('current_year')
mese_corrente = datetime.utcnow().month

# Leggi lista di company
companies_list = config.get('companies', [])

client_id = config.get('client_id_saas', '')
client_secret = config.get('client_secret_saas', '')
tenant_id = config.get('tenant_id_saas', '')

# Variabili globali
tokenSaas = ''
tokenExpirationTime = 0

# Email settings
sender_email = config.get('sender_email', '')
receiver_email = config.get('receiver_email', '')
password_mail = config.get('password_mail', '')

# Configura il logger
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Configura il contesto SSL
context = ssl.create_default_context()

############
# FUNZIONI #
############

# CREAZIONE CONTAINER AZURE

def crea_contenitore_se_non_esiste(container_name):
    try:
        container_client = blob_service_client.create_container(container_name)
        print(f"Contenitore '{container_name}' creato con successo.")
        # container = 'ok'
    except ResourceExistsError:
        print(f"Il contenitore '{container_name}' esiste già.")
        # container = 'alrt'
    # return container

# LETTURA PARQUET DA AZURE

# def cerca_e_leggi_parquet(target_file_name):
#     # Funzione per leggere un file Parquet da Azure Blob Storage in un DataFrame
#     def read_parquet_from_azure(container_name, blob_name):
#         container_client = blob_service_client.get_container_client(container_name)
#         blob_client = container_client.get_blob_client(blob_name)
#         stream = BytesIO()
#         blob_client.download_blob().readinto(stream)
#         stream.seek(0)
#         df = pd.read_parquet(stream)
#         df.reset_index(inplace=True)  # Resetta l'indice per assicurarti che tutte le colonne siano incluse
#         return df

#     # Cercare il file Parquet specificato in tutti i contenitori tranne "outputpbi"
#     containers = blob_service_client.list_containers()
#     for container in containers:
#         container_name = container['name']
#         if container_name == "outputpbi" or container_name == "newmodel":
#             continue  # Salta il contenitore "outputpbi"
#         container_client = blob_service_client.get_container_client(container_name)
#         blobs_list = container_client.list_blobs()
#         for blob in blobs_list:
#             if blob.name == target_file_name:
#                 return read_parquet_from_azure(container_name, blob.name)
#     return None  # Se il file non è trovato, restituisce None

def cerca_e_leggi_parquet(container_name):
    # Funzione per leggere tutti i file Parquet da un container Azure Blob Storage in un unico DataFrame
    def read_all_parquet_from_azure(container_name):
        container_client = blob_service_client.get_container_client(container_name)
        blob_list = container_client.list_blobs()
        
        # Lista per memorizzare i DataFrame
        df_list = []
        
        for blob in blob_list:
            blob_client = container_client.get_blob_client(blob.name)
            stream = BytesIO()
            blob_client.download_blob().readinto(stream)
            stream.seek(0)
            df = pd.read_parquet(stream)
            df_list.append(df)
        
        # Concatenare tutti i DataFrame in uno solo
        final_df = pd.concat(df_list, ignore_index=True)
        return final_df
    
    # Chiamata alla funzione interna
    return read_all_parquet_from_azure(container_name)

# SCRITTURA PARQUET AZURE

def svuota_container(container_name):
    """
    Elimina tutti i blob presenti in un container Azure.
    
    Args:
        container_name: nome del container da svuotare
    """
    try:
        container_client = blob_service_client.get_container_client(container_name)
        blob_list = container_client.list_blobs()
        
        for blob in blob_list:
            blob_client = container_client.get_blob_client(blob.name)
            blob_client.delete_blob()
            print(f"Eliminato blob: {blob.name} dal container {container_name}")
        
        print(f"Container '{container_name}' svuotato con successo.")
    except Exception as e:
        print(f"Errore durante lo svuotamento del container '{container_name}': {e}")

def carica_parquet_su_azure(df, container_name, blob_name):
    # Convertire il DataFrame in un buffer di memoria
    stream = BytesIO()
    df.to_parquet(stream, index=False)
    stream.seek(0)

    # Creare un client del blob
    blob_client = blob_service_client.get_blob_client(container=container_name, blob=blob_name)

    # Caricare il file Parquet nel blob
    blob_client.upload_blob(stream, overwrite=True)

# FETCH DATA FROM WEB SERVICE MOV - ON PREM

# Monkey-patch hashlib to support MD4
def md4_hash(data):
    h = MD4.new()
    h.update(data)
    return h

# Patch hashlib's new function for MD4
def patched_hashlib_new(name, data=None):
    if name.lower() == "md4":
        if data is None:
            return md4_hash(b"")
        return md4_hash(data)
    return original_hashlib_new(name, data)

# Backup original hashlib.new
original_hashlib_new = hashlib.new
hashlib.new = patched_hashlib_new

# Function to fetch data with pagination
def fetch_data(url, month, username, password):
    data_list = []
    session = requests.Session()
    while url:
        try:
            response = session.get(
                url,
                auth=HttpNtlmAuth(username, password),
                headers={
                    "Accept": "application/json",
                    "Content-Type": "application/json;charset=utf-8",
                    "Connection": "keep-alive",
                },
            )
            # Verify response status
            if response.status_code == 200:
                json_data = response.json()
                data_list.extend(json_data['value'])
                url = json_data.get('@odata.nextLink')  # Get the next link for pagination
                # rit = 'yes'
            else:
                print(f"Request failed for month {month} with status code: {response.status_code}")
                # rit = 'no'
                # print(response.text)
                break
        except Exception as e:
            print(f"An error occurred for month {month}: {e}")
            # rit = 'no'
            break
    return data_list #, rit

# FETCH DATA FROM WEB SERVICE ANAG - ON PREM

def fetch_data_anag(url, username, password):
    data_list = []
    session = requests.Session()
    while url:
        try:
            response = session.get(
                url,
                auth=HttpNtlmAuth(username, password),
                headers={
                    "Accept": "application/json",
                    "Content-Type": "application/json;charset=utf-8",
                    "Connection": "keep-alive",
                },
            )
            # Verifica lo stato della risposta
            if response.status_code == 200:
                json_data = response.json()
                data_list.extend(json_data['value'])
                url = json_data.get('@odata.nextLink')  # Ottieni il link successivo per la paginazione
            else:
                print(f"Request failed with status code: {response.status_code}")
                # print(response.text)
                break
        except Exception as e:
            print(f"An error occurred: {e}")
            break
    return data_list

# FETCH DATA FROM WEB SERVICE CUST - ON PREM

# def fetch_data_cust(url, username, password):
#     data_list = []
#     session = requests.Session()
#     while url:
#         try:
#             response = session.get(
#                 url,
#                 auth=HttpNtlmAuth(username, password),
#                 headers={
#                     "Accept": "application/json",
#                     "Content-Type": "application/json;charset=utf-8",
#                     "Connection": "keep-alive",
#                 },
#             )
#             # Verifica lo stato della risposta
#             if response.status_code == 200:
#                 json_data = response.json()
#                 data_list.extend(json_data['value'])
#                 url = json_data.get('@odata.nextLink')  # Ottieni il link successivo per la paginazione
#             else:
#                 print(f"Request failed with status code: {response.status_code}")
#                 # print(response.text)
#                 break
#         except Exception as e:
#             print(f"An error occurred: {e}")
#             break
#     return data_list

# FETCH DATA FROM WEB SERVICE - SAAS

def get_access_token(tenant_id, client_id, client_secret):
    global tokenSaas
    global tokenExpirationTime
    if (tokenSaas == '') or (tokenExpirationTime - 900 < time.time()):
        token_url = f"https://login.microsoftonline.com/{tenant_id}/oauth2/v2.0/token"
        data = {
            'grant_type': 'client_credentials',
            'client_id': client_id,
            'client_secret': client_secret,
            'scope': 'https://api.businesscentral.dynamics.com/.default'
        }
        response = requests.post(token_url, data=data)
        response.raise_for_status()
        tokenSaas = response.json().get('access_token')
        tokenExpirationTime = time.time() + response.json().get('expires_in')

# def fetch_data_from_bc_saas1(tenant_id, client_id, client_secret, api_endpoint):
#     global tokenSaas
    
#     # Ottieni il token di accesso
#     try:
#         get_access_token(tenant_id, client_id, client_secret)
#     except Exception as exc:
#         print(f'Errore nel recupero del token Oauth2: {exc}')
#         return None

#     # Recupera i dati dall'API di Business Central SaaS
#     try:
#         headers = {
#             'Authorization': f'Bearer {tokenSaas}',
#             'Accept': 'application/json',
#             'Content-Type': 'application/json;charset=utf-8',
#             'Connection': 'keep-alive'
#         }
#         response = requests.get(api_endpoint, headers=headers)
#         response.raise_for_status()
#     except Exception as exc:
#         print(f'Errore nel recupero dei dati: {exc}')
#         return None

#     data = response.json()
#     if not response.ok:
#         print(f'Errore nel recupero dei dati: {response.reason}')
#         return None

#     # Verifica la struttura dei dati
#     if 'value' in data:
#         return data['value']
#     else:
#         print("Formato dei dati inatteso:", data)
#         return None

# Funzione per recuperare i dati da Business Central SaaS con paginazione
def fetch_data_from_bc_saas(tenant_id, client_id, client_secret, api_endpoint):
    global tokenSaas

    # Ottieni il token di accesso
    try:
        get_access_token(tenant_id, client_id, client_secret)
    except Exception as exc:
        print(f'Errore nel recupero del token Oauth2: {exc}')
        return None

    all_data = []
    while api_endpoint:
        # Recupera i dati dall'API di Business Central SaaS
        try:
            headers = {
                'Authorization': f'Bearer {tokenSaas}',
                'Accept': 'application/json',
                'Content-Type': 'application/json;charset=utf-8',
                'Connection': 'keep-alive'
            }
            response = requests.get(api_endpoint, headers=headers)
            response.raise_for_status()
        except Exception as exc:
            print(f'Errore nel recupero dei dati: {exc}')
            return None

        data = response.json()
        if not response.ok:
            print(f'Errore nel recupero dei dati: {response.reason}')
            return None

        # Verifica la struttura dei dati
        if 'value' in data:
            all_data.extend(data['value'])
            # Verifica se c'è una pagina successiva
            api_endpoint = data.get('@odata.nextLink', None)
        else:
            print("Formato dei dati inatteso:", data)
            return None

    return all_data

######################
# LOOP SULLE COMPANY #
######################

# Inizializza log globali
all_log_data_agginc = []
all_log_data_storico = []
all_log_data_anag = []

# Cicla su tutte le company configurate
for company_config in companies_list:
    print(f"\n{'='*80}")
    print(f"ELABORAZIONE COMPANY: {company_config['name']}")
    print(f"{'='*80}\n")
    
    # Imposta variabili specifiche per la company
    Company = company_config['company_encoded']
    company_sanitized = company_config['sanitized']
    base_url = company_config['base_url']
    subject = company_config['name'] + " | Power BI - daily report"
    
    # Crea DataFrame con container specifici per questa company
    defServ = pd.DataFrame(columns=['Service', 'ColRif'])
    defServ_data = config.get('defServ_mov', '')
    defServ['Service'] = defServ_data
    defServ['ColRif'] = config.get('defServ_mov_colRif', '')
    defServ['transformed_string'] = defServ['Service'].apply(lambda x: x.replace("Qry", "").lower()) 
    defServ['transformed_string'] = defServ['transformed_string'].apply(lambda x: x.replace("_", "").lower())
    defServ['container_name_corr'] = defServ['transformed_string'] + "corrente-" + company_sanitized
    defServ['container_name_stor'] = defServ['transformed_string'] + "storico-" + company_sanitized

    defServ_anag = pd.DataFrame(columns=['Service'])
    defServ_anag['Service'] = config.get('defServ_anag', '')
    defServ_anag['ColRif'] = config.get('defServ_mov_colRif', '')
    defServ_anag['transformed_string'] = defServ_anag['Service'].apply(lambda x: x.replace("Qry", "").lower()) 
    defServ_anag['container_name_corr'] = defServ_anag['transformed_string'] + "corrente-" + company_sanitized

    # Svuota tutti i container prima di scrivere nuovi dati (per evitare dati sporchi)
    print(f"\n--- Svuotamento container per company: {company_config['name']} ---")
    containers_to_clear = set()
    
    # Aggiungi container movimenti
    for _, row in defServ.iterrows():
        for year in range(start_year, end_year + 1):
            if year >= current_year:
                containers_to_clear.add(row['container_name_corr'])
            else:
                containers_to_clear.add(row['container_name_stor'])
    
    # Aggiungi container anagrafiche
    for _, row in defServ_anag.iterrows():
        containers_to_clear.add(row['container_name_corr'])
    
    # Svuota i container
    for container_name in containers_to_clear:
        svuota_container(container_name)
    
    print(f"--- Fine svuotamento container ---\n")

    ############################################
    # AGGIORNAMENTO INCREMENTALE MOV - ON PREM #
    ############################################

    # for container_name in defServ['container_name_stor']:
    #     crea_contenitore_se_non_esiste(container_name)

    # for container_name in defServ['container_name_corr']:
    #     crea_contenitore_se_non_esiste(container_name)

    if BC_type == "ONPREM" and first_time == "no":
        log_data_agginc = []
        for _, row in defServ.iterrows():
            original_string = row['Service']
            col_rif = row['ColRif']  # Questo corrisponde a VE_PostingDate
    
            # Iterare sugli anni
            for year in range(start_year, end_year + 1):
                # Determinare il contenitore corretto in base all'anno
                if year >= current_year:
                    container_name = row['container_name_corr']
                else:
                    container_name = row['container_name_stor']
    
                # Preparare i contenitori se non esistono
                # crea_contenitore_se_non_esiste(container_name)
                try:
                    crea_contenitore_se_non_esiste(container_name)
                    log_data_agginc.append({'Ora': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                                        'Fase': 'AGG INC',
                                        'Azione': 'Creazione container',
                                        'Contenitore': container_name,
                                        'Risultato': 'success',
                                        'Dettaglio': ''})
                except Exception as e:
                    log_data_agginc.append({'Ora': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                                            'Fase': 'AGG INC',
                                            'Azione': 'Creazione container',
                                            'Contenitore': container_name,
                                            'Risultato': 'error',
                                            'Dettaglio': str(e)})
                # log_message = f"Creato il contenitore: {container_name}"
                # logging.info(log_message)
                # log_data_agginc.append({'Ora': datetime.now().strftime('%Y-%m-%d %H:%M:%S'), 'Contenitore': container_name, 'Successo': crea_contenitore_se_non_esiste(container_name)})
                # df_log = pd.DataFrame(log_data_agginc)
                # html_table = df_log.to_html(index=False)
    
                
    
                # Ottieni il mese corrente solo per il 2024, altrimenti tutti i 12 mesi
                mesi = range(1, datetime.utcnow().month + 1) if year >= end_year else range(1, 13)
                # mesi = range(10, 11) if year >= 2024 else range(10, 11)
                # mesi = range(1, 3) if year == 2024 else range(1, 13)
                # DataFrame vuoto per accumulare i dati di tutti i mesi
                df_total = pd.DataFrame()
    
                with ThreadPoolExecutor(max_workers=5) as executor:
                    tasks = []
                    for month in mesi:
                        start_date = datetime(year, month, 1)
                        end_date = datetime(year, month, calendar.monthrange(year, month)[1])
                        ora = datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ")
                        
                        # Convert dates to string format
                        start_date_str = start_date.strftime("%Y-%m-%dT00:00:00Z")
                        end_date_str = end_date.strftime("%Y-%m-%dT23:59:59Z")
                        if end_date_str >= ora and year >= end_year:
                        # if end_date_str >= ora and year >= 2024:
                            end_date_str = ora
    
                        # Construct OData filter query for the current month
                        filter_query = f"?$filter={col_rif} ge {start_date_str} and {col_rif} lt {(end_date + timedelta(days=1)).strftime('%Y-%m-%dT00:00:00Z')}&$select=SystemId,SystemModifiedAt"
                        url = base_url + original_string + filter_query
    
                        # Submit task to executor, passing month and year as additional data
                        tasks.append((executor.submit(fetch_data, url, month, username, password), month, year))
    
                    # Collect results
                    for future, month, year in tasks:
                        try:
                            all_data = future.result()
    
                            # Convert the data to a DataFrame
                            df = pd.DataFrame(all_data)
    
                            log_data_agginc.append({'Ora': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                                                'Fase': 'AGG INC',
                                                'Azione': 'Call ws',
                                                'Contenitore': container_name,
                                                'Risultato': 'success',
                                                'Dettaglio': f"Letto ws per {original_string} - {year}-{str(month).zfill(2)}"})
    
                            # Nome del file Parquet da leggere dal container
                            month_str = f"{year}_{str(month).zfill(2)}"
                            parquet_file_name = f"{original_string}_{month_str}.parquet"
                            try: 
                                # Leggere il Parquet esistente nel container Azure
                                blob_service_client = BlobServiceClient.from_connection_string(connection_string)
                                blob_client = blob_service_client.get_blob_client(container_name, parquet_file_name)
    
                                # Scaricare il file Parquet come oggetto byte
                                parquet_data = blob_client.download_blob().readall()
    
                                # Caricare il Parquet in un DataFrame
                                parquet_df = pd.read_parquet(io.BytesIO(parquet_data))
                            except Exception as e:    
                                carica_parquet_su_azure(df, container_name, parquet_file_name)
    
                                log_data_agginc.append({'Ora': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                                            'Fase': 'AGG INC',
                                            'Azione': 'No data in Azure',
                                            'Contenitore': container_name,
                                            'Risultato': 'success',
                                            'Dettaglio': f"Scritto ws storico per {original_string} - {year}-{str(month).zfill(2)}"})
    
                                blob_service_client = BlobServiceClient.from_connection_string(connection_string)
                                blob_client = blob_service_client.get_blob_client(container_name, parquet_file_name)
    
                                # Scaricare il file Parquet come oggetto byte
                                parquet_data = blob_client.download_blob().readall()
    
                                # Caricare il Parquet in un DataFrame
                                parquet_df = pd.read_parquet(io.BytesIO(parquet_data))
    
                            # Estrarre la colonna 'SystemId' dal Parquet esistente
                            existing_system_ids = parquet_df['SystemId'].values
    
                            # INSERT
    
                            # Filtrare i dati di df per mantenere solo i nuovi SystemId
                            df_new = df[~df['SystemId'].isin(existing_system_ids)]
                            if not df_new.empty:
                                new_system_ids = df_new['SystemId'].tolist()
                                # Costruisci un filtro per recuperare i dati completi dei nuovi SystemId
                                
                                batch_size = 50  # Puoi regolare questa dimensione in base al limite del server
                                batches = [new_system_ids[i:i + batch_size] for i in range(0, len(new_system_ids), batch_size)]
                                # batches = [new_system_ids[i:i + batch_size] for i in range(0, 1)]
    
                                # DataFrame vuoto per accumulare i dati dettagliati
                                df_detailed_total = pd.DataFrame()
    
                                for batch in batches:
                                    # Costruisci un filtro per il batch corrente
                                    id_filters = " or ".join([f"SystemId eq {id_}" for id_ in batch])
                                    detailed_filter_query = f"?$filter={id_filters}"
                                    detailed_url = base_url + original_string + detailed_filter_query
    
                                    # Richiama il web service per ottenere i dati completi del batch
                                    detailed_data = fetch_data(detailed_url, month, username, password)
    
                                    # Converti i dati completi del batch in un DataFrame e accodalo
                                    df_detailed_batch = pd.DataFrame(detailed_data)
                                    df_detailed_total = pd.concat([df_detailed_total, df_detailed_batch], ignore_index=True)
                                    
                                df_updated = pd.concat([parquet_df, df_detailed_total], ignore_index=True)
                                min_non_null_columns = 3  # Ad esempio, almeno 3 colonne non vuote
                                df_updated = df_updated.dropna(thresh=min_non_null_columns).reset_index(drop=True)
                                # Accoda i dati nuovi al DataFrame totale
                                # df_total = pd.concat([df_total, df_new], ignore_index=True)
                                carica_parquet_su_azure(df_updated, container_name, parquet_file_name)
                                print(f"Inseriti dati per {original_string} - {year}-{str(month).zfill(2)}")
    
                                log_data_agginc.append({'Ora': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                                                'Fase': 'AGG INC',
                                                'Azione': 'Insert new data from BC',
                                                'Contenitore': container_name,
                                                'Risultato': 'success',
                                                'Dettaglio': f"Inseriti {len(df_detailed_total)} dati per {original_string} - {year}-{str(month).zfill(2)}"})
                            
                            # Debug: mostra i dati raccolti
                            # print(f"Raccolti dati per {original_string} - {year}-{str(month).zfill(2)}")
    
                            # # Copia l'ultima riga
                            # last_row = df_updated.iloc[-1].copy()
    
                            # # Modifica il campo 'X'
                            # last_row['SystemId'] = 'RECORDPROVA'
    
                            # # Aggiungi la nuova riga al DataFrame
                            # df_updated0 = pd.concat([df_updated, pd.DataFrame([last_row])], ignore_index=True)
    
                            # DELETE
    
                            blob_service_client = BlobServiceClient.from_connection_string(connection_string)
                            blob_client = blob_service_client.get_blob_client(container_name, parquet_file_name)
    
                            # Scaricare il file Parquet come oggetto byte
                            parquet_data = blob_client.download_blob().readall()
    
                            # Caricare il Parquet in un DataFrame
                            parquet_df = pd.read_parquet(io.BytesIO(parquet_data))
    
                            # Estrarre la colonna 'SystemId' dal Parquet esistente
                            existing_system_ids = parquet_df['SystemId'].values
    
                            # Controlla se ci sono SystemId mancanti nel df che sono invece nel Parquet
                            missing_system_ids = existing_system_ids[~pd.Series(existing_system_ids).isin(df['SystemId'].values)]
    
                            # Se ci sono SystemId mancanti, crea un DataFrame per salvarli
                            if len(missing_system_ids) > 0:
                                # df_missing = pd.DataFrame(missing_system_ids, columns=['SystemId'])
    
                                # Salvataggio dei SystemId mancanti in un file CSV o Parquet
                                # missing_file_name = f"{original_string}_{year}_{month}_missing_system_ids.csv"
                                # df_missing.to_parquet(missing_file_name)  # Se vuoi salvare come Parquet
                                # df_missing.to_csv(missing_file_name, index=False)  # Se vuoi salvare come CSV
                                # print(f"SystemId mancanti salvati in {missing_file_name}")
                                df_filtered = parquet_df[~parquet_df['SystemId'].isin(missing_system_ids)]
    
                                if not df_filtered.empty:
                                    # Sovrascrivere il file Parquet su Azure
                                    carica_parquet_su_azure(df_filtered, container_name, parquet_file_name)
                                    print(f"Cancellati dati per {original_string} - {year}-{str(month).zfill(2)}")
    
                                else:
                                    print(f"Nessun dato da eliminare per {parquet_file_name}. Nessun file salvato.")
                            
                                log_data_agginc.append({'Ora': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                                                'Fase': 'AGG INC',
                                                'Azione': 'Delete data from BC',
                                                'Contenitore': container_name,
                                                'Risultato': 'success',
                                                'Dettaglio': f"Cancellati {len(missing_system_ids)} dati per {original_string} - {year}-{str(month).zfill(2)}"})
                            # UPDATE                        
    
                            blob_service_client = BlobServiceClient.from_connection_string(connection_string)
                            blob_client = blob_service_client.get_blob_client(container_name, parquet_file_name)
    
                            # Scaricare il file Parquet come oggetto byte
                            parquet_data = blob_client.download_blob().readall()
    
                            # Caricare il Parquet in un DataFrame
                            parquet_df = pd.read_parquet(io.BytesIO(parquet_data))
    
                            # Estrarre la colonna 'SystemId' dal Parquet esistente
                            existing_system_ids = parquet_df['SystemId'].values
    
                            # Copia l'ultima riga
                            # last_row = parquet_df.iloc[-1].copy()
    
                            # # # # Modifica il campo 'Timestamp' aggiungendo un giorno
                            # last_timestamp = datetime.strptime(last_row['SystemModifiedAt'], '%Y-%m-%dT%H:%M:%S.%fZ')
                            # new_timestamp = last_timestamp + timedelta(days=1)
                            # # # df_updated.at[df_updated.index[-1], 'SystemModifiedAt'] = '2024-12-12T08:34:22.5100Z'
                            # parquet_df.at[parquet_df.index[-1], 'SystemModifiedAt'] = new_timestamp.strftime('%Y-%m-%dT%H:%M:%S.%fZ')[:-3] + 'Z'
    
                            # Unire i DataFrame df e parquet_df sui SystemId
                            merged_df = df.merge(parquet_df[['SystemId', 'SystemModifiedAt']], on='SystemId', how='left', suffixes=('', '_parquet'))
    
                            # Filtrare le righe in cui 'SystemModifiedAt' è diverso tra df e parquet_df
                            df_diff_modified = merged_df[merged_df['SystemModifiedAt_parquet'].notna() & (merged_df['SystemModifiedAt'] != merged_df['SystemModifiedAt_parquet'])]
    
                            # Se ci sono differenze, crea il DataFrame e salvalo
                            if not df_diff_modified.empty:
                                system_ids_to_update = df_diff_modified['SystemId'].values
                                id_filters = " or ".join([f"SystemId eq {id_}" for id_ in system_ids_to_update])
                                filter_query = f"?$filter={id_filters}"
                                filter_url = base_url + original_string + filter_query
    
                                filter_data = fetch_data(filter_url, month, username, password)
                                filter_data = pd.DataFrame(filter_data)
                                remaining_rows = parquet_df[~parquet_df['SystemId'].isin(system_ids_to_update)]
    
                                updated_parquet_df = pd.concat([remaining_rows, filter_data], ignore_index=True)
    
                                min_non_null_columns = 3  # Ad esempio, almeno 3 colonne non vuote
                                updated_parquet_df = updated_parquet_df.dropna(thresh=min_non_null_columns).reset_index(drop=True)
                                carica_parquet_su_azure(updated_parquet_df, container_name, parquet_file_name)
    
                                print(f"Modificati dati per {original_string} - {year}-{str(month).zfill(2)}")
                                
                                log_data_agginc.append({'Ora': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                                                'Fase': 'AGG INC',
                                                'Azione': 'Update data from BC',
                                                'Contenitore': container_name,
                                                'Risultato': 'success',
                                                'Dettaglio': f"Modificati {len(df_diff_modified)} dati per {original_string} - {year}-{str(month).zfill(2)}"})
    
    
                                # df_diff_modified = df_diff_modified[['SystemId', 'SystemModifiedAt', 'SystemModifiedAt_parquet']]
                                # diff_file_name = f"{original_string}_{year}_{month}_modified_diff.csv"
                                # # df_diff_modified.to_csv(diff_file_name, index=False)
                                # print(f"Differenze 'SystemModifiedAt' salvate in {diff_file_name}")
    
                        except Exception as e:
                            print(f"Errore durante l'elaborazione dei dati per {original_string} - {year}-{month}: {e}")
                            log_data_agginc.append({'Ora': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                                                'Fase': 'AGG INCR',
                                                'Azione': f'No data from ws {year}-{month}',
                                                'Contenitore': container_name,
                                                'Risultato': 'error',
                                                'Dettaglio': str(e)})
    
                # Salvataggio finale del DataFrame completo
                # month_str = f"{year}"
                # parquet_file_name = f"{original_string}_{month_str}.parquet"
                # carica_parquet_su_azure(df_total, container_name, parquet_file_name)
                # print(f"Dati totali raccolti e salvati per {original_string} - {month_str}")
    
        df_log_agginc = pd.DataFrame(log_data_agginc)
        df_error_agginc = df_log_agginc[(df_log_agginc['Risultato'] == 'error') & (~df_log_agginc['Azione'].str.contains('No data', na=False))]
        html_table_agginc = df_error_agginc.to_html(index=False)
        html_table_agginc_log = df_log_agginc.to_html(index=False)
    else:
        html_table_agginc = None
        html_table_agginc_log = None
    
    ################################
    # POPOLA STORICO MOV - ON PREM #
    ################################
    
    if BC_type == "ONPREM" and first_time == "yes":
        log_data_storico = []
        for _, row in defServ.iterrows():
            original_string = row['Service']
            col_rif = row['ColRif']  # Questo corrisponde a VE_PostingDate
    
            all_months_success = True
    
            # Iterare sugli anni
            for year in range(start_year, end_year + 1):
                # Determinare il contenitore corretto in base all'anno
                if year >= current_year:
                    container_name = row['container_name_corr']
                else:
                    container_name = row['container_name_stor']
    
                # Preparare i contenitori se non esistono
                # crea_contenitore_se_non_esiste(container_name)
    
                try:
                    crea_contenitore_se_non_esiste(container_name)
                    log_data_storico.append({'Ora': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                                        'Fase': 'STORICO',
                                        'Azione': 'Creazione container',
                                        'Contenitore': container_name,
                                        'Risultato': 'success',
                                        'Dettaglio': ''})
                except Exception as e:
                    log_data_storico.append({'Ora': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                                            'Fase': 'STORICO',
                                            'Azione': 'Creazione container',
                                            'Contenitore': container_name,
                                            'Risultato': 'error',
                                            'Dettaglio': str(e)})
    
                # Ottieni il mese corrente solo per il 2024, altrimenti tutti i 12 mesi
                mesi = range(1, datetime.utcnow().month + 1) if year >= end_year else range(1, 13)
    
                tasks = []
    
                with ThreadPoolExecutor(max_workers=5) as executor:
                    for month in mesi:
                        start_date = datetime(year, month, 1)
                        end_date = datetime(year, month, calendar.monthrange(year, month)[1])
                        ora = datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ")
                        # Convert dates to string format
                        start_date_str = start_date.strftime("%Y-%m-%dT00:00:00Z")
                        end_date_str = end_date.strftime("%Y-%m-%dT23:59:59Z")
                        if end_date_str >= ora and year >= end_year:
                            end_date_str = ora
    
                        # Construct OData filter query for the current month
                        filter_query = f"?$filter={col_rif} ge {start_date_str} and {col_rif} lt {(end_date + timedelta(days=1)).strftime('%Y-%m-%dT00:00:00Z')}"
                        url = base_url + original_string + filter_query
    
                        # Submit task to executor, passing month and year as additional data
                        tasks.append((executor.submit(fetch_data, url, month, username, password), month, year))
    
                    # Collect results
                    for future, month, year in tasks:
                        try:
                            all_data = future.result()
                            # Convert the data to a DataFrame
                            df = pd.DataFrame(all_data)
    
                            if df.empty:
                                log_data_storico.append({
                                    'Ora': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                                    'Fase': 'STORICO',
                                    'Azione': f'No data from ws {year}-{month}',
                                    'Contenitore': container_name,
                                    'Risultato': 'error',
                                    'Dettaglio': f"No data from ws per {original_string} - {year}-{str(month).zfill(2)}"
                                })
                                continue  # Salta il salvataggio e vai al prossimo mese
    
                            log_data_storico.append({'Ora': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                                                'Fase': 'STORICO',
                                                'Azione': 'Call ws',
                                                'Contenitore': container_name,
                                                'Risultato': 'success',
                                                'Dettaglio': f"Letto ws per {original_string} - {year}-{str(month).zfill(2)}"})
    
                            # Save DataFrame to a parquet file
                            month_str = f"{year}_{str(month).zfill(2)}"
                            parquet_file_name = f"{original_string}_{month_str}.parquet"
                            carica_parquet_su_azure(df, container_name, parquet_file_name)
                            print(f"Dati raccolti e salvati per {original_string} - {month_str}")
                            log_data_storico.append({'Ora': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                                                'Fase': 'STORICO',
                                                'Azione': 'Write data',
                                                'Contenitore': container_name,
                                                'Risultato': 'success',
                                                'Dettaglio': f"Scritti dati per {original_string} - {year}-{str(month).zfill(2)}"})
                        except Exception as e:
                            all_months_success = False
                            print(f"An error occurred while processing data for {original_string} - {year}-{month}: {e}")
                            log_data_storico.append({'Ora': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                                                'Fase': 'STORICO',
                                                'Azione': f'No data from ws {year}-{month}',
                                                'Contenitore': container_name,
                                                'Risultato': 'error',
                                                'Dettaglio': str(e)})
                
                # if df_total.empty:
                #     # print(f"Nessun dato trovato per {original_string}, creando un file vuoto.")
                #     df_total = pd.DataFrame({"No data": [0]})  # Sostituisci con i nomi delle colonne effettive
    
        df_log_storico = pd.DataFrame(log_data_storico)
        df_error_storico = df_log_storico[(df_log_storico['Risultato'] == 'error') & (~df_log_storico['Azione'].str.contains('No data', na=False))]
        html_table_storico = df_error_storico.to_html(index=False)
        html_table_storico_log = df_log_storico.to_html(index=False)
    else:
        html_table_storico = None
        html_table_storico_log = None
    
    #########################
    # ANAGRAFICHE - ON PREM #
    #########################
    
    if BC_type == "ONPREM":
        log_data_anag = []
        for _, row in defServ_anag.iterrows():
            original_string = row['Service']
            col_rif = row['ColRif']  # Questo corrisponde a VE_PostingDate
            container_name = row['container_name_corr']
    
            # Preparare i contenitori se non esistono
            # crea_contenitore_se_non_esiste(container_name)
    
            try:
                crea_contenitore_se_non_esiste(container_name)
                log_data_anag.append({'Ora': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                                    'Fase': 'ANAG',
                                    'Azione': 'Creazione container',
                                    'Contenitore': container_name,
                                    'Risultato': 'success',
                                    'Dettaglio': ''})
            except Exception as e:
                log_data_anag.append({'Ora': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                                        'Fase': 'ANAG',
                                        'Azione': 'Creazione container',
                                        'Contenitore': container_name,
                                        'Risultato': 'error',
                                        'Dettaglio': str(e)})
    
            # Inizializza il DataFrame totale per accumulare i dati di tutti i servizi
            df_total = pd.DataFrame()
    
            with ThreadPoolExecutor(max_workers=5) as executor:
                tasks = []
    
                # Costruire la query OData per il servizio corrente
                ora = datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ")
                filter_query = f"?$filter={col_rif} lt {ora}"
                url = base_url + original_string + filter_query
    
                # Submit task to executor
                tasks.append(executor.submit(fetch_data_anag, url, username, password))
    
                # Collect results
                for future in tasks:
                    try:
                        all_data = future.result()
    
                        # Convert the data to a DataFrame
                        df = pd.DataFrame(all_data)
    
                        # Accoda i dati al DataFrame totale
                        df_total = pd.concat([df_total, df], ignore_index=True)
    
                        print(f"Raccolti dati per {original_string}")
                        log_data_anag.append({'Ora': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                                                'Fase': 'ANAG',
                                                'Azione': 'Call ws',
                                                'Contenitore': container_name,
                                                'Risultato': 'success',
                                                'Dettaglio': f"Letto ws per {original_string}"})
    
    
                    except Exception as e:
                        print(f"Errore durante l'elaborazione dei dati per {original_string}: {e}")
                        log_data_anag.append({'Ora': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                                                'Fase': 'ANAG',
                                                'Azione': f'No data from ws',
                                                'Contenitore': container_name,
                                                'Risultato': 'error',
                                                'Dettaglio': str(e)})
            
            if df_total.empty:
                # print(f"Nessun dato trovato per {original_string}, creando un file vuoto.")
                df_total = pd.DataFrame({"No data": [0]})  # Sostituisci con i nomi delle colonne effettive
                log_data_anag.append({'Ora': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                                                'Fase': 'ANAG',
                                                'Azione': 'Write data',
                                                'Contenitore': container_name,
                                                'Risultato': 'success',
                                                'Dettaglio': f"Nessun dato tovato per {original_string}"})
    
            parquet_file_name = f"{original_string}.parquet"
            carica_parquet_su_azure(df_total, container_name, parquet_file_name)
            print(f"Dati totali raccolti e salvati per {original_string}")
            log_data_anag.append({'Ora': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                                                'Fase': 'ANAG',
                                                'Azione': 'Write data',
                                                'Contenitore': container_name,
                                                'Risultato': 'success',
                                                'Dettaglio': f"Scritti dati per {original_string}"})
    
        df_log_anag = pd.DataFrame(log_data_anag)
        df_error_anag = df_log_anag[(df_log_anag['Risultato'] == 'error') & (~df_log_anag['Azione'].str.contains('No data', na=False))]
        html_table_anag = df_error_anag.to_html(index=False)
        html_table_anag_log = df_log_anag.to_html(index=False)
    else:
        html_table_anag = None
        html_table_anag_log = None
    
    #############
    # SEND MAIL #
    #############
    
    if BC_type == "ONPREM":
        # Inizializza il messaggio
        messagg = ''
    
        # Verifica se i DataFrame esistono e sono vuoti
        anag_vuota = 'df_error_anag' in locals() and df_error_anag.empty
        agginc_vuota = 'df_error_agginc' in locals() and df_error_agginc.empty
        storico_vuota = 'df_error_storico' in locals() and df_error_storico.empty
    
        # Genera il messaggio basato sulla presenza e sul contenuto dei DataFrame
        if anag_vuota and agginc_vuota and storico_vuota:
            messagg = 'Tutti i dati sono aggiornati correttamente'
        else:
            messaggi = []
            if 'df_error_anag' in locals() and not df_error_anag.empty:
                messaggi.append('Errore nei dati anagrafici')
            if 'df_error_agginc' in locals() and not df_error_agginc.empty:
                messaggi.append('Errore nei dati di aggiornamento incrementale')
            if 'df_error_storico' in locals() and not df_error_storico.empty:
                messaggi.append('Errore nei dati storici')
    
            if messaggi:
                messagg = ', '.join(messaggi)
            else:
                messagg = 'Tutti i dati sono aggiornati correttamente'
    
        # print(messagg)
    
        # Definisci le icone per i messaggi
        green_icon = "&#x2705;"  # Emoji "White Heavy Check Mark"
        red_icon = "&#x274C;"  # Emoji "Cross Mark"
    
        # Verifica se le tabelle sono vuote e aggiorna i titoli
        html_table_storico_title = '<h5>Tabella dei fatti - POPOLA CONTAINER FT - ' + ('nessun errore rilevato' if html_table_storico is not None and df_error_storico.empty else 'errori rilevati') + '</h5>'
        html_table_agginc_title = '<h5>Tabella dei fatti - AGG INC - ' + ('nessun errore rilevato' if html_table_agginc is not None and df_error_agginc.empty else 'errori rilevati') + '</h5>'
        html_table_anag_title = '<h5>Dimensioni - ' + ('nessun errore rilevato' if html_table_anag is not None and df_error_anag.empty else 'errori rilevati') + '</h5>'
    
        body = f"""
        <html>
        <head>
            <link href="https://fonts.googleapis.com/css?family=Open+Sans&display=swap" rel="stylesheet">
            <style>
                body {{
                    font-family: 'Open Sans', sans-serif;
                }}
                h2 {{
                    color: #333;
                }}
                table {{
                    width: 100%;
                    border-collapse: collapse;
                }}
                th, td {{
                    border: 1px solid #ddd;
                    padding: 8px;
                    font-size: 0.8em;
                }}
                th {{
                    background-color: #f2f2f2;
                    text-align: center;
                    font-size: 0.8em;
                }}
                td{{
                    font-size: 0.8em;
                }}
                tr:nth-child(even) {{
                    background-color: #f9f9f9;
                }}
                .message {{
                    font-size: 1.2em;
                    color: {'green' if messagg == 'Tutti i dati sono aggiornati correttamente' else 'red'};
                    margin-bottom: 20px;
                }}
            </style>
        </head>
        <body>
            <div class="message">
                {green_icon if messagg == 'Tutti i dati sono aggiornati correttamente' else red_icon} {messagg}
            </div>
            { html_table_storico_title + html_table_storico if html_table_storico is not None else '' }
            { html_table_agginc_title + html_table_agginc if html_table_agginc is not None else '' }
            { html_table_anag_title + html_table_anag if html_table_anag is not None else '' }
        </body>
        </html>
        """
    
        # body = f"""
        # <html>
        # <head>
        #     <link href="https://fonts.googleapis.com/css?family=Open+Sans&display=swap" rel="stylesheet">
        #     <style>
        #         body {{
        #             font-family: 'Open Sans', sans-serif;
        #         }}
        #         h2 {{
        #             color: #333;
        #         }}
        #         table {{
        #             width: 100%;
        #             border-collapse: collapse;
        #         }}
        #         th, td {{
        #             border: 1px solid #ddd;
        #             padding: 8px;
        #             font-size: 0.8em;
        #         }}
        #         th {{
        #             background-color: #f2f2f2;
        #             text-align: center;
        #             font-size: 0.8em;
        #         }}
        #         td{{
        #             font-size: 0.8em;
        #         }}
        #         tr:nth-child(even) {{
        #             background-color: #f9f9f9;
        #         }}
        #         .message {{
        #             font-size: 1.2em;
        #             color: {'green' if messagg == 'Tutti i dati sono aggiornati correttamente' else 'red'};
        #             margin-bottom: 20px;
        #         }}
        #     </style>
        # </head>
        # <body>
        #     <div><div class="message">
        #         {green_icon if messagg == 'Tutti i dati sono aggiornati correttamente' else red_icon} {messagg}
        #     </div><h5>Tabella dei fatti - POPOLA CONTAINER FT - nessun errore rilevato</h5><table class="x_dataframe" border="1"><thead><tr style="text-align:right"><th>Ora</th><th>Fase</th><th>Azione</th><th>Contenitore</th><th>Risultato</th><th>Dettaglio</th></tr></thead><tbody></tbody></table><h5>Dimensioni - nessun errore rilevato</h5><table class="x_dataframe" border="1"><thead><tr style="text-align:right"><th>Ora</th><th>Fase</th><th>Azione</th><th>Contenitore</th><th>Risultato</th><th>Dettaglio</th></tr></thead><tbody></tbody></table><p><br><br><span style="font-size: xx-small; font-family: Arial, Helvetica, sans-serif, serif, EmojiFont; color: rgb(153, 153, 153);">Questo messaggio e' stato analizzato con Libraesva ESG ed e' risultato non infetto. <br><a data-auth="NotApplicable" rel="noopener noreferrer" target="_blank" href="https://mail.serinf.it/action/4Y7tQt23LpzJmhS/report-as-bad" title="https://mail.serinf.it/action/4Y7tQt23LpzJmhS/report-as-bad" data-linkindex="0">Clicca qui per segnalarlo come spam.</a></span> <span style="font-size: xx-small; font-family: Arial, Helvetica, sans-serif, serif, EmojiFont; color: rgb(153, 153, 153);"><br><a data-auth="NotApplicable" rel="noopener noreferrer" target="_blank" href="https://mail.serinf.it/action/4Y7tQt23LpzJmhS/blocklist" title="https://mail.serinf.it/action/4Y7tQt23LpzJmhS/blocklist" data-linkindex="1">Clicca qui per metterlo in blocklist</a></span></p></div>
        # </body>
        # </html>
        # """
    
        # Invia l'email
        with smtplib.SMTP_SSL("smtp.gmail.com", 465, context=context) as server:
            server.login(sender_email, password_mail)
            message = f"Subject: {subject}\nContent-Type: text/html\n\n{body}"
            server.sendmail(sender_email, receiver_email, message)
    
        #######
        # LOG #
        #######
    
        body_log = f"""
        <html>
        <head>
            <link href="https://fonts.googleapis.com/css?family=Open+Sans&display=swap" rel="stylesheet">
            <style>
                body {{
                    font-family: 'Open Sans', sans-serif;
                }}
                h2 {{
                    color: #333;
                }}
                table {{
                    width: 100%;
                    border-collapse: collapse;
                }}
                th, td {{
                    border: 1px solid #ddd;
                    padding: 8px;
                    font-size: 0.8em;
                }}
                th {{
                    background-color: #f2f2f2;
                    text-align: center;
                }}
                tr:nth-child(even) {{
                    background-color: #f9f9f9;
                }}
                .message {{
                    font-size: 1.2em;
                    color: {'green' if messagg == 'Tutti i dati sono aggiornati correttamente' else 'red'};
                    margin-bottom: 20px;
                }}
            </style>
        </head>
        <body>
            <div class="message" font-size: 1.2em;
                    color: {'green' if messagg == 'Tutti i dati sono aggiornati correttamente' else 'red'};
                    margin-bottom: 20px;>
                {green_icon if messagg == 'Tutti i dati sono aggiornati correttamente' else red_icon} {messagg}
            </div>
            <p>{Company + " | Power BI - daily report"}</p>
            { html_table_storico_title + html_table_storico_log if html_table_storico_log is not None else '' }
            { html_table_agginc_title + html_table_agginc_log if html_table_agginc_log is not None else '' }
            { html_table_anag_title + html_table_anag_log if html_table_anag_log is not None else '' }
        </body>
        </html>
        """
    
        # with smtplib.SMTP_SSL("smtp.gmail.com", 465, context=context) as server:
        #     server.login(sender_email, password_mail)
        #     message = f"Subject: {subject}\nContent-Type: text/html\n\n{body1}"
        #     server.sendmail(sender_email, receiver_email, message)
    
        # import smtplib
        # from email.mime.multipart import MIMEMultipart
        # from email.mime.text import MIMEText
        # from email.mime.application import MIMEApplication
        # import ssl
    
        # # Invia l'email con l'allegato HTML e il messaggio
        # def send_email_with_attachment(subject, body_message, sender_email, receiver_email, password_mail, attachment_content):
        #     message = MIMEMultipart()
        #     message['From'] = sender_email
        #     message['To'] = receiver_email
        #     message['Subject'] = subject
    
        #     # Corpo dell'email con il messaggio
        #     message_body = f"<div class='message'>{green_icon if messagg == 'Tutti i dati sono aggiornati correttamente' else red_icon} {messagg}</div>"
        #     message.attach(MIMEText(message_body, 'html'))
    
        #     # Allegato HTML
        #     attachment = MIMEApplication(attachment_content, _subtype="html")
        #     attachment.add_header('Content-Disposition', 'attachment', filename="report.html")
        #     message.attach(attachment)
    
        #     # Invia l'email
        #     context = ssl.create_default_context()
        #     with smtplib.SMTP_SSL("smtp.gmail.com", 465, context=context) as server:
        #         server.login(sender_email, password_mail)
        #         server.sendmail(sender_email, receiver_email, message.as_string())
    
        # # Esempio di utilizzo
        # send_email_with_attachment(subject, "", sender_email, receiver_email, password_mail, body1)
    
        with open(os.path.join(base_path, f"daily-log-{company_sanitized}.html"), "w", encoding="utf-8") as file: 
            file.write(body_log)
        
        # Accumula i log per questa company
        if 'log_data_agginc' in locals():
            all_log_data_agginc.extend(log_data_agginc)
        if 'log_data_storico' in locals():
            all_log_data_storico.extend(log_data_storico)
        if 'log_data_anag' in locals():
            all_log_data_anag.extend(log_data_anag)
    
    
    ######################
    # ANAGRAFICHE - SAAS #
    ######################
    
    if BC_type == "SAAS":
        log_data_anag = []
        for _, row in defServ_anag.iterrows():
            original_string = row['Service']
            col_rif = row['ColRif']
            container_name = row['container_name_corr']
            try:
                crea_contenitore_se_non_esiste(container_name)
                log_data_anag.append({'Ora': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                                    'Fase': 'ANAG',
                                    'Azione': 'Creazione container',
                                    'Contenitore': container_name,
                                    'Risultato': 'success',
                                    'Dettaglio': ''})
            except Exception as e:
                log_data_anag.append({'Ora': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                                        'Fase': 'ANAG',
                                        'Azione': 'Creazione container',
                                        'Contenitore': container_name,
                                        'Risultato': 'error',
                                        'Dettaglio': str(e)})
    
            df_total = pd.DataFrame()
    
            with ThreadPoolExecutor(max_workers=5) as executor:
                tasks = []
    
                ora = datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ")
                filter_query = f"?$filter={col_rif} lt {ora}"
                url = base_url + original_string + filter_query
    
                tasks.append(executor.submit(fetch_data_from_bc_saas, tenant_id, client_id, client_secret, url))
    
                for future in tasks:
                    try:
                        all_data = future.result()
    
                        if all_data:
                            df = pd.DataFrame(all_data)
                            df_total = pd.concat([df_total, df], ignore_index=True)
    
                            print(f"Raccolti dati per {original_string}")
                            log_data_anag.append({'Ora': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                                                'Fase': 'ANAG',
                                                'Azione': 'Call ws',
                                                'Contenitore': container_name,
                                                'Risultato': 'success',
                                                'Dettaglio': f"Letto ws per {original_string}"})
                        else:
                            print(f"Nessun dato trovato per {original_string}")
    
                    except Exception as e:
                        print(f"Errore durante l'elaborazione dei dati per {original_string}: {e}")
                        log_data_anag.append({'Ora': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                                                'Fase': 'ANAG',
                                                'Azione': f'No data from ws',
                                                'Contenitore': container_name,
                                                'Risultato': 'error',
                                                'Dettaglio': str(e)})
            
            if df_total.empty:
                # print(f"Nessun dato trovato per {original_string}, creando un file vuoto.")
                df_total = pd.DataFrame({"No data": [0]})  # Sostituisci con i nomi delle colonne effettive
                log_data_anag.append({'Ora': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                                                'Fase': 'ANAG',
                                                'Azione': 'Write data',
                                                'Contenitore': container_name,
                                                'Risultato': 'success',
                                                'Dettaglio': f"Nessun dato tovato per {original_string}"})
    
            parquet_file_name = f"{original_string}.parquet"
            carica_parquet_su_azure(df_total, container_name, parquet_file_name)
            print(f"Dati totali raccolti e salvati per {original_string}")
            log_data_anag.append({'Ora': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                                                'Fase': 'ANAG',
                                                'Azione': 'Write data',
                                                'Contenitore': container_name,
                                                'Risultato': 'success',
                                                'Dettaglio': f"Scritti dati per {original_string}"})
            
        df_log_anag = pd.DataFrame(log_data_anag)
        df_error_anag = df_log_anag[(df_log_anag['Risultato'] == 'error') & (~df_log_anag['Azione'].str.contains('No data', na=False))]
        html_table_anag = df_error_anag.to_html(index=False)
        html_table_anag_log = df_log_anag.to_html(index=False)
    else:
        html_table_anag = None
        html_table_anag_log = None
    
    #############################
    # POPOLA STORICO MOV - SAAS #
    #############################
    
    results = []
    
    if BC_type == "SAAS" and first_time == "yes":
        log_data_storico = []
        for _, row in defServ.iterrows():
            original_string = row['Service']
            col_rif = row['ColRif']
    
            all_months_success = True
            
            # Iterare sugli anni
            for year in range(start_year, end_year + 1):
                if year >= current_year:
                    container_name = row['container_name_corr']
                else:
                    container_name = row['container_name_stor']
                
                # Creare contenitore se non esiste
                try:
                    crea_contenitore_se_non_esiste(container_name)
                    log_data_storico.append({'Ora': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                                        'Fase': 'STORICO',
                                        'Azione': 'Creazione container',
                                        'Contenitore': container_name,
                                        'Risultato': 'success',
                                        'Dettaglio': ''})
                except Exception as e:
                    log_data_storico.append({'Ora': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                                            'Fase': 'STORICO',
                                            'Azione': 'Creazione container',
                                            'Contenitore': container_name,
                                            'Risultato': 'error',
                                            'Dettaglio': str(e)})
                
                mesi = range(1, datetime.utcnow().month + 1) if year >= end_year else range(1, 13)
                tasks = []
    
                with ThreadPoolExecutor(max_workers=5) as executor:
                    for month in mesi:
                        # Imposta intervallo temporale per il mese
                        start_date = datetime(year, month, 1)
                        end_date = datetime(year, month, calendar.monthrange(year, month)[1])
                        ora = datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ")
                        
                        # Formattazione delle date per la query
                        start_date_str = start_date.strftime("%Y-%m-%dT00:00:00Z")
                        end_date_str = end_date.strftime("%Y-%m-%dT23:59:59Z")
                        if end_date_str >= ora and year >= end_year:
                            end_date_str = ora
    
                        # Costruzione della query
                        filter_query = f"?$filter={col_rif} ge {start_date_str} and {col_rif} lt {end_date_str}"
                        url = base_url + "/" + original_string + filter_query
    
                        # Task per il mese
                        tasks.append(executor.submit(fetch_data_from_bc_saas, tenant_id, client_id, client_secret, url))
    
                    # Elaborazione dei risultati
                    for future, month in zip(tasks, mesi):
                        try:
                            all_data = future.result()
                            if all_data:
                                # Creazione del DataFrame
                                df = pd.DataFrame(all_data)
    
                                if df.empty:
                                    log_data_storico.append({
                                        'Ora': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                                        'Fase': 'STORICO',
                                        'Azione': f'No data from ws {year}-{month}',
                                        'Contenitore': container_name,
                                        'Risultato': 'error',
                                        'Dettaglio': f"No data from ws per {original_string} - {year}-{str(month).zfill(2)}"
                                    })
                                    continue  # Salta il salvataggio e vai al prossimo mese
    
                                log_data_storico.append({'Ora': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                                                    'Fase': 'STORICO',
                                                    'Azione': 'Call ws',
                                                    'Contenitore': container_name,
                                                    'Risultato': 'success',
                                                    'Dettaglio': f"Letto ws per {original_string} - {year}-{str(month).zfill(2)}"})
                                
                                # Nome file mensile
                                month_str = f"{year}_{str(month).zfill(2)}"
                                parquet_file_name = f"{original_string}_{month_str}.parquet"
                                
                                # Caricamento dati su Azure
                                carica_parquet_su_azure(df, container_name, parquet_file_name)
                                print(f"Dati raccolti e salvati per {original_string} - {month_str}")
                                log_data_storico.append({'Ora': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                                                'Fase': 'STORICO',
                                                'Azione': 'Write data',
                                                'Contenitore': container_name,
                                                'Risultato': 'success',
                                                'Dettaglio': f"Scritti dati per {original_string} - {year}-{str(month).zfill(2)}"})
                            # else:
                            #     print(f"Nessun dato trovato per {original_string} - {year}-{month}")
                        except Exception as e:
                            all_months_success = False
                            print(f"Errore durante l'elaborazione dei dati per {original_string} - {year}-{month}: {e}")
                            log_data_storico.append({'Ora': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                                                'Fase': 'STORICO',
                                                'Azione': f'No data from ws {year}-{month}',
                                                'Contenitore': container_name,
                                                'Risultato': 'error',
                                                'Dettaglio': str(e)})
                
        df_log_storico = pd.DataFrame(log_data_storico)
        df_error_storico = df_log_storico[(df_log_storico['Risultato'] == 'error') & (~df_log_storico['Azione'].str.contains('No data', na=False))]
        html_table_storico = df_error_storico.to_html(index=False)
        html_table_storico_log = df_log_storico.to_html(index=False)
    else:
        html_table_storico = None
        html_table_storico_log = None
    
    #########################################
    # AGGIORNAMENTO INCREMENTALE MOV - SAAS #
    #########################################
    
    if BC_type == "SAAS" and first_time == "no":
        log_data_agginc = []
        for _, row in defServ.iterrows():
            original_string = row['Service']
            col_rif = row['ColRif']
            
            # Iterare sugli anni
            for year in range(start_year, end_year + 1):
                if year >= current_year:
                    container_name = row['container_name_corr']
                else:
                    container_name = row['container_name_stor']
                
                try:
                    crea_contenitore_se_non_esiste(container_name)
                    log_data_agginc.append({'Ora': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                                        'Fase': 'AGG INC',
                                        'Azione': 'Creazione container',
                                        'Contenitore': container_name,
                                        'Risultato': 'success',
                                        'Dettaglio': ''})
                except Exception as e:
                    log_data_agginc.append({'Ora': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                                            'Fase': 'AGG INC',
                                            'Azione': 'Creazione container',
                                            'Contenitore': container_name,
                                            'Risultato': 'error',
                                            'Dettaglio': str(e)})
                
                mesi = range(1, datetime.utcnow().month + 1) if year >= end_year else range(1, 13)
                # mesi = range(1, 2)
                tasks = []
    
                with ThreadPoolExecutor(max_workers=5) as executor:
                    for month in mesi:
                        # Imposta intervallo temporale per il mese
                        start_date = datetime(year, month, 1)
                        end_date = datetime(year, month, calendar.monthrange(year, month)[1])
                        ora = datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ")
                        
                        # Formattazione delle date per la query
                        start_date_str = start_date.strftime("%Y-%m-%dT00:00:00Z")
                        end_date_str = end_date.strftime("%Y-%m-%dT23:59:59Z")
                        if end_date_str >= ora and year >= end_year:
                            end_date_str = ora
    
                        # Costruzione della query
                        filter_query = f"?$filter={col_rif} ge {start_date_str} and {col_rif} lt {end_date_str}"
                        url = base_url + "/" + original_string + filter_query
    
                        # Task per il mese
                        tasks.append(executor.submit(fetch_data_from_bc_saas, tenant_id, client_id, client_secret, url))
    
                    # Elaborazione dei risultati
                    for future, month in zip(tasks, mesi):
                        try:
                            all_data = future.result()
                            if all_data:
                                # Creazione del DataFrame
                                df = pd.DataFrame(all_data)
    
                                log_data_agginc.append({'Ora': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                                                'Fase': 'AGG INC',
                                                'Azione': 'Call ws',
                                                'Contenitore': container_name,
                                                'Risultato': 'success',
                                                'Dettaglio': f"Letto ws per {original_string} - {year}-{str(month).zfill(2)}"})
                                
                                # Nome file mensile
                                month_str = f"{year}_{str(month).zfill(2)}"
                                parquet_file_name = f"{original_string}_{month_str}.parquet"
    
                                try: 
                                    # Leggere il Parquet esistente nel container Azure
                                    blob_service_client = BlobServiceClient.from_connection_string(connection_string)
                                    blob_client = blob_service_client.get_blob_client(container_name, parquet_file_name)
    
                                    # Scaricare il file Parquet come oggetto byte
                                    parquet_data = blob_client.download_blob().readall()
    
                                    # Caricare il Parquet in un DataFrame
                                    parquet_df = pd.read_parquet(io.BytesIO(parquet_data))
                                
                                except Exception as e:    
                                    carica_parquet_su_azure(df, container_name, parquet_file_name)
    
                                    log_data_agginc.append({'Ora': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                                                'Fase': 'AGG INC',
                                                'Azione': 'No data in Azure',
                                                'Contenitore': container_name,
                                                'Risultato': 'success',
                                                'Dettaglio': f"Scritto ws storico per {original_string} - {year}-{str(month).zfill(2)}"})
    
                                    blob_service_client = BlobServiceClient.from_connection_string(connection_string)
                                    blob_client = blob_service_client.get_blob_client(container_name, parquet_file_name)
    
                                    # Scaricare il file Parquet come oggetto byte
                                    parquet_data = blob_client.download_blob().readall()
    
                                    # Caricare il Parquet in un DataFrame
                                    parquet_df = pd.read_parquet(io.BytesIO(parquet_data))
    
                                # Estrarre la colonna 'SystemId' dal Parquet esistente
                                existing_system_ids = parquet_df['SystemId'].values
                                
                                # INSERT
    
                                # Filtrare i dati di df per mantenere solo i nuovi SystemId
                                df_new = df[~df['SystemId'].isin(existing_system_ids)]
                                if not df_new.empty:
                                    new_system_ids = df_new['SystemId'].tolist()
                                    # Costruisci un filtro per recuperare i dati completi dei nuovi SystemId
                                    
                                    batch_size = 50  # Puoi regolare questa dimensione in base al limite del server
                                    batches = [new_system_ids[i:i + batch_size] for i in range(0, len(new_system_ids), batch_size)]
                                    # batches = [new_system_ids[i:i + batch_size] for i in range(0, 1)]
    
                                    # DataFrame vuoto per accumulare i dati dettagliati
                                    df_detailed_total = pd.DataFrame()
    
                                    for batch in batches:
                                        # Costruisci un filtro per il batch corrente
                                        id_filters = " or ".join([f"SystemId eq {id_}" for id_ in batch])
                                        detailed_filter_query = f"?$filter={id_filters}"
                                        detailed_url = base_url + "/" + original_string + detailed_filter_query
    
                                        # Richiama il web service per ottenere i dati completi del batch
                                        detailed_data = fetch_data_from_bc_saas(tenant_id, client_id, client_secret, detailed_url)
    
                                        # Converti i dati completi del batch in un DataFrame e accodalo
                                        df_detailed_batch = pd.DataFrame(detailed_data)
                                        df_detailed_total = pd.concat([df_detailed_total, df_detailed_batch], ignore_index=True)
                                    
                                    df_updated = pd.concat([parquet_df, df_detailed_total], ignore_index=True)
                                    min_non_null_columns = 3  # Ad esempio, almeno 3 colonne non vuote
                                    df_updated = df_updated.dropna(thresh=min_non_null_columns).reset_index(drop=True)
                                    # Accoda i dati nuovi al DataFrame totale
                                    # df_total = pd.concat([df_total, df_new], ignore_index=True)
                                    carica_parquet_su_azure(df_updated, container_name, parquet_file_name)
                                    print(f"Inseriti dati per {original_string} - {year}-{str(month).zfill(2)}")
    
                                    log_data_agginc.append({'Ora': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                                                'Fase': 'AGG INC',
                                                'Azione': 'Insert new data from BC',
                                                'Contenitore': container_name,
                                                'Risultato': 'success',
                                                'Dettaglio': f"Inseriti {len(df_detailed_total)} dati per {original_string} - {year}-{str(month).zfill(2)}"})
    
                                # Debug: mostra i dati raccolti
                                # print(f"Raccolti dati per {original_string} - {year}-{str(month).zfill(2)}")
                                
                                # DELETE
    
                                blob_service_client = BlobServiceClient.from_connection_string(connection_string)
                                blob_client = blob_service_client.get_blob_client(container_name, parquet_file_name)
    
                                # Scaricare il file Parquet come oggetto byte
                                parquet_data = blob_client.download_blob().readall()
    
                                # Caricare il Parquet in un DataFrame
                                parquet_df = pd.read_parquet(io.BytesIO(parquet_data))
    
                                # Estrarre la colonna 'SystemId' dal Parquet esistente
                                existing_system_ids = parquet_df['SystemId'].values
    
                                # Controlla se ci sono SystemId mancanti nel df che sono invece nel Parquet
                                missing_system_ids = existing_system_ids[~pd.Series(existing_system_ids).isin(df['SystemId'].values)]
    
                                # Se ci sono SystemId mancanti, crea un DataFrame per salvarli
                                if len(missing_system_ids) > 0:
                                    # df_missing = pd.DataFrame(missing_system_ids, columns=['SystemId'])
    
                                    # Salvataggio dei SystemId mancanti in un file CSV o Parquet
                                    # missing_file_name = f"{original_string}_{year}_{month}_missing_system_ids.csv"
                                    # df_missing.to_parquet(missing_file_name)  # Se vuoi salvare come Parquet
                                    # df_missing.to_csv(missing_file_name, index=False)  # Se vuoi salvare come CSV
                                    # print(f"SystemId mancanti salvati in {missing_file_name}")
                                    df_filtered = parquet_df[~parquet_df['SystemId'].isin(missing_system_ids)]
    
                                    if not df_filtered.empty:
                                        # Sovrascrivere il file Parquet su Azure
                                        carica_parquet_su_azure(df_filtered, container_name, parquet_file_name)
                                        print(f"Cancellati dati per {original_string} - {year}-{str(month).zfill(2)}")
                                    else:
                                        print(f"Tutti i record sono stati eliminati dal Parquet per {parquet_file_name}. Nessun file salvato.")
                                        
                                    log_data_agginc.append({'Ora': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                                                'Fase': 'AGG INC',
                                                'Azione': 'Delete data from BC',
                                                'Contenitore': container_name,
                                                'Risultato': 'success',
                                                'Dettaglio': f"Cancellati {len(missing_system_ids)} dati per {original_string} - {year}-{str(month).zfill(2)}"})
                            
                                # UPDATE
    
                                blob_service_client = BlobServiceClient.from_connection_string(connection_string)
                                blob_client = blob_service_client.get_blob_client(container_name, parquet_file_name)
    
                                # Scaricare il file Parquet come oggetto byte
                                parquet_data = blob_client.download_blob().readall()
    
                                # Caricare il Parquet in un DataFrame
                                parquet_df = pd.read_parquet(io.BytesIO(parquet_data))
    
                                # Estrarre la colonna 'SystemId' dal Parquet esistente
                                existing_system_ids = parquet_df['SystemId'].values
    
                                # Unire i DataFrame df e parquet_df sui SystemId
                                merged_df = df.merge(parquet_df[['SystemId', 'SystemModifiedAt']], on='SystemId', how='left', suffixes=('', '_parquet'))
    
                                # Filtrare le righe in cui 'SystemModifiedAt' è diverso tra df e parquet_df
                                df_diff_modified = merged_df[merged_df['SystemModifiedAt_parquet'].notna() & (merged_df['SystemModifiedAt'] != merged_df['SystemModifiedAt_parquet'])]
    
                                # Se ci sono differenze, crea il DataFrame e salvalo
                                if not df_diff_modified.empty:
                                    system_ids_to_update = df_diff_modified['SystemId'].values
                                    id_filters = " or ".join([f"SystemId eq {id_}" for id_ in system_ids_to_update])
                                    filter_query = f"?$filter={id_filters}"
                                    filter_url = base_url + original_string + filter_query
    
                                    filter_data = fetch_data_from_bc_saas(tenant_id, client_id, client_secret, filter_url)
                                    filter_data = pd.DataFrame(filter_data)
                                    remaining_rows = parquet_df[~parquet_df['SystemId'].isin(system_ids_to_update)]
    
                                    updated_parquet_df = pd.concat([remaining_rows, filter_data], ignore_index=True)
    
                                    min_non_null_columns = 3  # Ad esempio, almeno 3 colonne non vuote
                                    updated_parquet_df = updated_parquet_df.dropna(thresh=min_non_null_columns).reset_index(drop=True)
                                    carica_parquet_su_azure(updated_parquet_df, container_name, parquet_file_name)
    
                                    print(f"Modificati dati per {original_string} - {year}-{str(month).zfill(2)}")
    
                                    log_data_agginc.append({'Ora': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                                                'Fase': 'AGG INC',
                                                'Azione': 'Update data from BC',
                                                'Contenitore': container_name,
                                                'Risultato': 'success',
                                                'Dettaglio': f"Modificati {len(df_diff_modified)} dati per {original_string} - {year}-{str(month).zfill(2)}"})
    
                                
                                # df_diff_modified = df_diff_modified[['SystemId', 'SystemModifiedAt', 'SystemModifiedAt_parquet']]
                                # diff_file_name = f"{original_string}_{year}_{month}_modified_diff.csv"
                                # # df_diff_modified.to_csv(diff_file_name, index=False)
                                # print(f"Differenze 'SystemModifiedAt' salvate in {diff_file_name}")
    
                        except Exception as e:
                            print(f"Errore durante l'elaborazione dei dati per {original_string} - {year}-{month}: {e}")
                            log_data_agginc.append({'Ora': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                                                'Fase': 'AGG INCR',
                                                'Azione': f'No data from ws {year}-{month}',
                                                'Contenitore': container_name,
                                                'Risultato': 'error',
                                                'Dettaglio': str(e)})
    
        df_log_agginc = pd.DataFrame(log_data_agginc)
        df_error_agginc = df_log_agginc[(df_log_agginc['Risultato'] == 'error') & (~df_log_agginc['Azione'].str.contains('No data', na=False))]
        html_table_agginc = df_error_agginc.to_html(index=False)
        html_table_agginc_log = df_log_agginc.to_html(index=False)
    else:
        html_table_agginc = None
        html_table_agginc_log = None
    
    #############
    # SEND MAIL #
    #############
    
    if BC_type == "SAAS":
        # Inizializza il messaggio
        messagg = ''
    
        # Verifica se i DataFrame esistono e sono vuoti
        anag_vuota = 'df_error_anag' in locals() and df_error_anag.empty
        agginc_vuota = 'df_error_agginc' in locals() and df_error_agginc.empty
        storico_vuota = 'df_error_storico' in locals() and df_error_storico.empty
    
        # Genera il messaggio basato sulla presenza e sul contenuto dei DataFrame
        if anag_vuota and agginc_vuota and storico_vuota:
            messagg = 'Tutti i dati sono aggiornati correttamente'
        else:
            messaggi = []
            if 'df_error_anag' in locals() and not df_error_anag.empty:
                messaggi.append('Errore nei dati anagrafici')
            if 'df_error_agginc' in locals() and not df_error_agginc.empty:
                messaggi.append('Errore nei dati di aggiornamento incrementale')
            if 'df_error_storico' in locals() and not df_error_storico.empty:
                messaggi.append('Errore nei dati storici')
    
            if messaggi:
                messagg = ', '.join(messaggi)
            else:
                messagg = 'Tutti i dati sono aggiornati correttamente'
    
        # print(messagg)
    
        # Definisci le icone per i messaggi
        green_icon = "&#x2705;"  # Emoji "White Heavy Check Mark"
        red_icon = "&#x274C;"  # Emoji "Cross Mark"
    
        # Verifica se le tabelle sono vuote e aggiorna i titoli
        html_table_storico_title = '<h5>Tabella dei fatti - POPOLA CONTAINER FT - ' + ('nessun errore rilevato' if html_table_storico is not None and df_error_storico.empty else 'errori rilevati') + '</h5>'
        html_table_agginc_title = '<h5>Tabella dei fatti - AGG INC - ' + ('nessun errore rilevato' if html_table_agginc is not None and df_error_agginc.empty else 'errori rilevati') + '</h5>'
        html_table_anag_title = '<h5>Dimensioni - ' + ('nessun errore rilevato' if html_table_anag is not None and df_error_anag.empty else 'errori rilevati') + '</h5>'
    
        body = f"""
        <html>
        <head>
            <link href="https://fonts.googleapis.com/css?family=Open+Sans&display=swap" rel="stylesheet">
            <style>
                body {{
                    font-family: 'Open Sans', sans-serif;
                }}
                h2 {{
                    color: #333;
                }}
                table {{
                    width: 100%;
                    border-collapse: collapse;
                }}
                th, td {{
                    border: 1px solid #ddd;
                    padding: 8px;
                    font-size: 0.8em;
                }}
                th {{
                    background-color: #f2f2f2;
                    text-align: center;
                    font-size: 0.8em;
                }}
                td{{
                    font-size: 0.8em;
                }}
                tr:nth-child(even) {{
                    background-color: #f9f9f9;
                }}
                .message {{
                    font-size: 1.2em;
                    color: {'green' if messagg == 'Tutti i dati sono aggiornati correttamente' else 'red'};
                    margin-bottom: 20px;
                }}
            </style>
        </head>
        <body>
            <div class="message">
                {green_icon if messagg == 'Tutti i dati sono aggiornati correttamente' else red_icon} {messagg}
            </div>
            { html_table_storico_title + html_table_storico if html_table_storico is not None else '' }
            { html_table_agginc_title + html_table_agginc if html_table_agginc is not None else '' }
            { html_table_anag_title + html_table_anag if html_table_anag is not None else '' }
        </body>
        </html>
        """
    
        # body = f"""
        # <html>
        # <head>
        #     <link href="https://fonts.googleapis.com/css?family=Open+Sans&display=swap" rel="stylesheet">
        #     <style>
        #         body {{
        #             font-family: 'Open Sans', sans-serif;
        #         }}
        #         h2 {{
        #             color: #333;
        #         }}
        #         table {{
        #             width: 100%;
        #             border-collapse: collapse;
        #         }}
        #         th, td {{
        #             border: 1px solid #ddd;
        #             padding: 8px;
        #             font-size: 0.8em;
        #         }}
        #         th {{
        #             background-color: #f2f2f2;
        #             text-align: center;
        #             font-size: 0.8em;
        #         }}
        #         td{{
        #             font-size: 0.8em;
        #         }}
        #         tr:nth-child(even) {{
        #             background-color: #f9f9f9;
        #         }}
        #         .message {{
        #             font-size: 1.2em;
        #             color: {'green' if messagg == 'Tutti i dati sono aggiornati correttamente' else 'red'};
        #             margin-bottom: 20px;
        #         }}
        #     </style>
        # </head>
        # <body>
        #     <div><div class="message">
        #         {green_icon if messagg == 'Tutti i dati sono aggiornati correttamente' else red_icon} {messagg}
        #     </div><h5>Tabella dei fatti - POPOLA CONTAINER FT - nessun errore rilevato</h5><table class="x_dataframe" border="1"><thead><tr style="text-align:right"><th>Ora</th><th>Fase</th><th>Azione</th><th>Contenitore</th><th>Risultato</th><th>Dettaglio</th></tr></thead><tbody></tbody></table><h5>Dimensioni - nessun errore rilevato</h5><table class="x_dataframe" border="1"><thead><tr style="text-align:right"><th>Ora</th><th>Fase</th><th>Azione</th><th>Contenitore</th><th>Risultato</th><th>Dettaglio</th></tr></thead><tbody></tbody></table><p><br><br><span style="font-size: xx-small; font-family: Arial, Helvetica, sans-serif, serif, EmojiFont; color: rgb(153, 153, 153);">Questo messaggio e' stato analizzato con Libraesva ESG ed e' risultato non infetto. <br><a data-auth="NotApplicable" rel="noopener noreferrer" target="_blank" href="https://mail.serinf.it/action/4Y7tQt23LpzJmhS/report-as-bad" title="https://mail.serinf.it/action/4Y7tQt23LpzJmhS/report-as-bad" data-linkindex="0">Clicca qui per segnalarlo come spam.</a></span> <span style="font-size: xx-small; font-family: Arial, Helvetica, sans-serif, serif, EmojiFont; color: rgb(153, 153, 153);"><br><a data-auth="NotApplicable" rel="noopener noreferrer" target="_blank" href="https://mail.serinf.it/action/4Y7tQt23LpzJmhS/blocklist" title="https://mail.serinf.it/action/4Y7tQt23LpzJmhS/blocklist" data-linkindex="1">Clicca qui per metterlo in blocklist</a></span></p></div>
        # </body>
        # </html>
        # """
    
        # Invia l'email
        with smtplib.SMTP_SSL("smtp.gmail.com", 465, context=context) as server:
            server.login(sender_email, password_mail)
            message = f"Subject: {subject}\nContent-Type: text/html\n\n{body}"
            server.sendmail(sender_email, receiver_email, message)
    
        #######
        # LOG #
        #######
    
        body_log = f"""
        <html>
        <head>
            <link href="https://fonts.googleapis.com/css?family=Open+Sans&display=swap" rel="stylesheet">
            <style>
                body {{
                    font-family: 'Open Sans', sans-serif;
                }}
                h2 {{
                    color: #333;
                }}
                table {{
                    width: 100%;
                    border-collapse: collapse;
                }}
                th, td {{
                    border: 1px solid #ddd;
                    padding: 8px;
                    font-size: 0.8em;
                }}
                th {{
                    background-color: #f2f2f2;
                    text-align: center;
                }}
                tr:nth-child(even) {{
                    background-color: #f9f9f9;
                }}
                .message {{
                    font-size: 1.2em;
                    color: {'green' if messagg == 'Tutti i dati sono aggiornati correttamente' else 'red'};
                    margin-bottom: 20px;
                }}
            </style>
        </head>
        <body>
            <div class="message" font-size: 1.2em;
                    color: {'green' if messagg == 'Tutti i dati sono aggiornati correttamente' else 'red'};
                    margin-bottom: 20px;>
                {green_icon if messagg == 'Tutti i dati sono aggiornati correttamente' else red_icon} {messagg}
            </div>
            <p>{Company + " | Power BI - daily report"}</p>
            { html_table_storico_title + html_table_storico_log if html_table_storico_log is not None else '' }
            { html_table_agginc_title + html_table_agginc_log if html_table_agginc_log is not None else '' }
            { html_table_anag_title + html_table_anag_log if html_table_anag_log is not None else '' }
        </body>
        </html>
        """
    
        # with smtplib.SMTP_SSL("smtp.gmail.com", 465, context=context) as server:
        #     server.login(sender_email, password_mail)
        #     message = f"Subject: {subject}\nContent-Type: text/html\n\n{body1}"
        #     server.sendmail(sender_email, receiver_email, message)
    
        # import smtplib
        # from email.mime.multipart import MIMEMultipart
        # from email.mime.text import MIMEText
        # from email.mime.application import MIMEApplication
        # import ssl
    
        # # Invia l'email con l'allegato HTML e il messaggio
        # def send_email_with_attachment(subject, body_message, sender_email, receiver_email, password_mail, attachment_content):
        #     message = MIMEMultipart()
        #     message['From'] = sender_email
        #     message['To'] = receiver_email
        #     message['Subject'] = subject
    
        #     # Corpo dell'email con il messaggio
        #     message_body = f"<div class='message'>{green_icon if messagg == 'Tutti i dati sono aggiornati correttamente' else red_icon} {messagg}</div>"
        #     message.attach(MIMEText(message_body, 'html'))
    
        #     # Allegato HTML
        #     attachment = MIMEApplication(attachment_content, _subtype="html")
        #     attachment.add_header('Content-Disposition', 'attachment', filename="report.html")
        #     message.attach(attachment)
    
        #     # Invia l'email
        #     context = ssl.create_default_context()
        #     with smtplib.SMTP_SSL("smtp.gmail.com", 465, context=context) as server:
        #         server.login(sender_email, password_mail)
        #         server.sendmail(sender_email, receiver_email, message.as_string())
    
        # # Esempio di utilizzo
        # send_email_with_attachment(subject, "", sender_email, receiver_email, password_mail, body1)
    
        with open(os.path.join(base_path, f"daily-log-{company_sanitized}.html"), "w", encoding="utf-8") as file: 
            file.write(body_log)
        
        # Accumula i log per questa company
        if 'log_data_agginc' in locals():
            all_log_data_agginc.extend(log_data_agginc)
        if 'log_data_storico' in locals():
            all_log_data_storico.extend(log_data_storico)
        if 'log_data_anag' in locals():
            all_log_data_anag.extend(log_data_anag)
    
# Fine loop company

######################
# EMAIL RIEPILOGATIVA #
######################

# # Crea report finale con tutti i log
# if BC_type == "SAAS" or BC_type == "ONPREM":
#     # Crea DataFrame finali
#     df_all_log_agginc = pd.DataFrame(all_log_data_agginc) if all_log_data_agginc else pd.DataFrame()
#     df_all_log_storico = pd.DataFrame(all_log_data_storico) if all_log_data_storico else pd.DataFrame()
#     df_all_log_anag = pd.DataFrame(all_log_data_anag) if all_log_data_anag else pd.DataFrame()
#     
#     # Filtra errori
#     df_all_error_agginc = df_all_log_agginc[(df_all_log_agginc['Risultato'] == 'error') & (~df_all_log_agginc['Azione'].str.contains('No data', na=False))] if not df_all_log_agginc.empty else pd.DataFrame()
#     df_all_error_storico = df_all_log_storico[(df_all_log_storico['Risultato'] == 'error') & (~df_all_log_storico['Azione'].str.contains('No data', na=False))] if not df_all_log_storico.empty else pd.DataFrame()
#     df_all_error_anag = df_all_log_anag[(df_all_log_anag['Risultato'] == 'error') & (~df_all_log_anag['Azione'].str.contains('No data', na=False))] if not df_all_log_anag.empty else pd.DataFrame()
#     
#     # Verifica errori
#     anag_vuota = df_all_error_anag.empty
#     agginc_vuota = df_all_error_agginc.empty
#     storico_vuota = df_all_error_storico.empty
#     
#     if anag_vuota and agginc_vuota and storico_vuota:
#         messagg_final = 'Tutti i dati sono aggiornati correttamente per tutte le company'
#     else:
#         messaggi = []
#         if not df_all_error_anag.empty:
#             messaggi.append('Errore nei dati anagrafici')
#         if not df_all_error_agginc.empty:
#             messaggi.append('Errore nei dati di aggiornamento incrementale')
#         if not df_all_error_storico.empty:
#             messaggi.append('Errore nei dati storici')
#         messagg_final = ', '.join(messaggi) if messaggi else 'Tutti i dati sono aggiornati correttamente per tutte le company'
#     
#     # Icone email
#     green_icon = "&#x2705;"
#     red_icon = "&#x274C;"
#     
#     # Email riepilogativa
#     subject_final = "Power BI - Report giornaliero tutte le company"
#     
#     body_final = f"""
#     <html>
#     <head>
#         <link href="https://fonts.googleapis.com/css?family=Open+Sans&display=swap" rel="stylesheet">
#         <style>
#             body {{
#                 font-family: 'Open Sans', sans-serif;
#             }}
#             h2 {{
#                 color: #333;
#             }}
#             table {{
#                 width: 100%;
#                 border-collapse: collapse;
#             }}
#             th, td {{
#                 border: 1px solid #ddd;
#                 padding: 8px;
#                 font-size: 0.8em;
#             }}
#             th {{
#                 background-color: #f2f2f2;
#                 text-align: center;
#                 font-size: 0.8em;
#             }}
#             td{{
#                 font-size: 0.8em;
#             }}
#             tr:nth-child(even) {{
#                 background-color: #f9f9f9;
#             }}
#             .message {{
#                 font-size: 1.2em;
#                 color: {'green' if anag_vuota and agginc_vuota and storico_vuota else 'red'};
#                 margin-bottom: 20px;
#             }}
#         </style>
#     </head>
#     <body>
#         <div class="message">
#             {green_icon if anag_vuota and agginc_vuota and storico_vuota else red_icon} {messagg_final}
#         </div>
#         <h5>Errori - Tabella dei fatti STORICO</h5>
#         {df_all_error_storico.to_html(index=False) if not df_all_error_storico.empty else '<p>Nessun errore</p>'}
#         <h5>Errori - Tabella dei fatti AGG INC</h5>
#         {df_all_error_agginc.to_html(index=False) if not df_all_error_agginc.empty else '<p>Nessun errore</p>'}
#         <h5>Errori - Dimensioni</h5>
#         {df_all_error_anag.to_html(index=False) if not df_all_error_anag.empty else '<p>Nessun errore</p>'}
#     </body>
#     </html>
#     """
#     
#     # Invia email finale
#     with smtplib.SMTP_SSL("smtp.gmail.com", 465, context=context) as server:
#         server.login(sender_email, password_mail)
#         message = f"Subject: {subject_final}\nContent-Type: text/html\n\n{body_final}"
#         server.sendmail(sender_email, receiver_email, message)

print(f"\n{'='*80}")
print("ELABORAZIONE COMPLETATA PER TUTTE LE COMPANY")
print(f"{'='*80}\n")

