import ssl
from requests_ntlm import HttpNtlmAuth
import requests
import csv
import hashlib
from Crypto.Hash import MD4
import pandas as pd
from datetime import datetime, timedelta
from concurrent.futures import ThreadPoolExecutor, as_completed
from io import BytesIO
import calendar
import uuid
import os
import json
import io
import html
import time
import sys
from cryptography.fernet import Fernet
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

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

def trova_file_configurazione():
    candidate_dirs = []

    # Directory dell'eseguibile in caso di build frozen
    if getattr(sys, 'frozen', False):
        candidate_dirs.append(os.path.dirname(sys.executable))

    # Directory dello script quando __file__ e' disponibile
    if '__file__' in globals():
        candidate_dirs.append(os.path.dirname(os.path.abspath(__file__)))

    # Directory da cui viene lanciato il comando
    candidate_dirs.append(os.getcwd())

    # Directory del target passato a python (se presente)
    if len(sys.argv) > 0 and sys.argv[0]:
        candidate_dirs.append(os.path.dirname(os.path.abspath(sys.argv[0])))

    # Rimuovi duplicati preservando l'ordine
    dirs_uniche = []
    for d in candidate_dirs:
        if d and d not in dirs_uniche:
            dirs_uniche.append(d)

    percorsi_tentati = []
    for d in dirs_uniche:
        candidate = os.path.join(d, 'configuration.json')
        percorsi_tentati.append(candidate)
        if os.path.isfile(candidate):
            return candidate

    raise FileNotFoundError(
        "configuration.json non trovato. Percorsi tentati: " + " | ".join(percorsi_tentati)
    )


config_file_path = trova_file_configurazione()
base_path = os.path.dirname(config_file_path)
print(f"Percorso del file di configurazione: {config_file_path}")

with open(config_file_path, 'r', encoding='utf-8') as f:
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
local_output_path = config.get('local_output_path', r"C:\BC-BOARD")
os.makedirs(local_output_path, exist_ok=True)


class ResourceExistsError(Exception):
    pass


class LocalBlobDownload:
    def __init__(self, file_path):
        self.file_path = file_path

    def readall(self):
        with open(self.file_path, 'rb') as f:
            return f.read()

    def readinto(self, stream):
        with open(self.file_path, 'rb') as f:
            stream.write(f.read())


class LocalBlobClient:
    def __init__(self, file_path):
        self.file_path = file_path

    def download_blob(self):
        return LocalBlobDownload(self.file_path)

    def upload_blob(self, stream, overwrite=True):
        if (not overwrite) and os.path.exists(self.file_path):
            raise ResourceExistsError(f"Il file '{self.file_path}' esiste gia'.")
        os.makedirs(os.path.dirname(self.file_path), exist_ok=True)
        stream.seek(0)
        with open(self.file_path, 'wb') as f:
            f.write(stream.read())

    def delete_blob(self):
        if os.path.exists(self.file_path):
            os.remove(self.file_path)


class LocalBlobInfo:
    def __init__(self, name):
        self.name = name


class LocalContainerClient:
    def __init__(self, root_path, container_name):
        self.container_path = os.path.join(root_path, container_name)

    def list_blobs(self):
        if not os.path.exists(self.container_path):
            return []
        blobs = []
        for name in os.listdir(self.container_path):
            file_path = os.path.join(self.container_path, name)
            if os.path.isfile(file_path) and name.lower().endswith('.parquet'):
                blobs.append(LocalBlobInfo(name))
        return blobs

    def get_blob_client(self, blob_name):
        return LocalBlobClient(os.path.join(self.container_path, blob_name))


class BlobServiceClient:
    def __init__(self, root_path):
        self.root_path = root_path

    @classmethod
    def from_connection_string(cls, _connection_string):
        return cls(local_output_path)

    def create_container(self, container_name):
        container_path = os.path.join(self.root_path, container_name)
        if os.path.exists(container_path):
            raise ResourceExistsError(f"Il contenitore '{container_name}' esiste gia'.")
        os.makedirs(container_path, exist_ok=False)
        return LocalContainerClient(self.root_path, container_name)

    def get_container_client(self, container_name):
        return LocalContainerClient(self.root_path, container_name)

    def get_blob_client(self, container=None, blob=None):
        if container is None or blob is None:
            raise ValueError("container e blob sono obbligatori")
        return LocalBlobClient(os.path.join(self.root_path, container, blob))


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
export_full_txt = config.get('export_full_txt', True)
full_export_max_workers = int(config.get('full_export_max_workers', 4))
incremental_compare_enabled = config.get('incremental_compare_enabled', True)
incremental_compare_services = set(config.get('incremental_compare_services', ['GLEntry']))
incremental_batch_size = int(config.get('incremental_batch_size', 100))
incremental_batch_workers = int(config.get('incremental_batch_workers', 8))
incremental_mode = config.get('incremental_mode', 'watermark')
incremental_watermark_overlap_minutes = int(config.get('incremental_watermark_overlap_minutes', 15))
incremental_partitioned_months_back = int(config.get('incremental_partitioned_months_back', 2))
incremental_partitioned_full_reconcile_every = int(config.get('incremental_partitioned_full_reconcile_every', 0))
incremental_skip_delete_scan_services = set(config.get('incremental_skip_delete_scan_services', []))
incremental_changelog_delete_services = set(config.get('incremental_changelog_delete_services', ['GLEntry']))
incremental_changelog_service_name = config.get('incremental_changelog_service_name', 'ChangeLogEntry')
incremental_changelog_systemid_field = config.get('incremental_changelog_systemid_field', 'ChangedRecordSystemId')
incremental_changelog_datetime_field = config.get('incremental_changelog_datetime_field', 'DateandTime')
incremental_storage_format = str(config.get('incremental_storage_format', 'parquet')).strip().lower()
use_parquet_incremental = incremental_storage_format == 'parquet'
incremental_keys_format = str(
    config.get('incremental_keys_format', 'parquet' if use_parquet_incremental else 'txt')
).strip().lower()
use_parquet_keys = incremental_keys_format == 'parquet'
odata_maxpagesize = int(config.get('odata_maxpagesize', 2000))

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


def _create_http_session():
    session = requests.Session()
    retry = Retry(
        total=3,
        connect=3,
        read=3,
        backoff_factor=0.5,
        status_forcelist=(429, 500, 502, 503, 504),
        allowed_methods=frozenset(['GET'])
    )
    adapter = HTTPAdapter(pool_connections=20, pool_maxsize=20, max_retries=retry)
    session.mount('http://', adapter)
    session.mount('https://', adapter)
    session.headers.update({
        'Accept': 'application/json',
        'Content-Type': 'application/json;charset=utf-8',
        'Connection': 'keep-alive',
        'Accept-Encoding': 'gzip, deflate',
        'Prefer': f'odata.maxpagesize={odata_maxpagesize}'
    })
    return session


def _response_json_or_none(response, source_label):
    content_type = response.headers.get('Content-Type', '')
    body = response.text or ''
    if not body.strip():
        print(f"{source_label}: risposta vuota (status {response.status_code}, content-type {content_type})")
        return None
    if 'json' not in content_type.lower() and not body.lstrip().startswith('{') and not body.lstrip().startswith('['):
        preview = body[:500].replace('\n', ' ').replace('\r', ' ')
        print(f"{source_label}: risposta non JSON (status {response.status_code}, content-type {content_type}) - preview: {preview}")
        return None
    try:
        return response.json()
    except ValueError as exc:
        preview = body[:500].replace('\n', ' ').replace('\r', ' ')
        print(f"{source_label}: impossibile decodificare JSON (status {response.status_code}, content-type {content_type}) - {exc} - preview: {preview}")
        return None

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
    session = _create_http_session()
    while url:
        try:
            response = session.get(
                url,
                auth=HttpNtlmAuth(username, password),
            )
            # Verify response status
            if response.status_code == 200:
                json_data = _response_json_or_none(response, f"ONPREM {month}")
                if json_data is None:
                    break
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
    session = _create_http_session()
    while url:
        try:
            response = session.get(
                url,
                auth=HttpNtlmAuth(username, password),
            )
            # Verifica lo stato della risposta
            if response.status_code == 200:
                json_data = _response_json_or_none(response, "ANAG ONPREM")
                if json_data is None:
                    break
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
        token_payload = _response_json_or_none(response, 'OAUTH TOKEN')
        if token_payload is None:
            raise ValueError('Risposta token OAuth non valida')
        tokenSaas = token_payload.get('access_token')
        tokenExpirationTime = time.time() + token_payload.get('expires_in')

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
    session = _create_http_session()
    while api_endpoint:
        # Recupera i dati dall'API di Business Central SaaS
        try:
            headers = {
                'Authorization': f'Bearer {tokenSaas}',
                'Accept': 'application/json',
                'Content-Type': 'application/json;charset=utf-8',
                'Connection': 'keep-alive',
                'Accept-Encoding': 'gzip, deflate',
                'Prefer': f'odata.maxpagesize={odata_maxpagesize}'
            }
            response = session.get(api_endpoint, headers=headers, timeout=120)
            response.raise_for_status()
        except Exception as exc:
            print(f'Errore nel recupero dei dati: {exc}')
            return None

        data = _response_json_or_none(response, 'SAAS BC')
        if data is None:
            return None
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


def build_service_url(base_url, service_name):
    return f"{base_url.rstrip('/')}/{service_name}"


def salva_dataframe_txt(df, output_folder, service_name):
    os.makedirs(output_folder, exist_ok=True)
    output_file = os.path.join(output_folder, f"{service_name}.txt")
    if df is None or df.empty:
        # Crea comunque il file per mantenere un output consistente per ogni tabella.
        with open(output_file, 'w', encoding='utf-8'):
            pass
        return output_file

    df.to_csv(output_file, sep='\t', index=False, encoding='utf-8')
    return output_file


def salva_dataframe_parquet(df, output_folder, service_name):
    os.makedirs(output_folder, exist_ok=True)
    output_file = os.path.join(output_folder, f"{service_name}.parquet")
    df_out = df if df is not None else pd.DataFrame()
    df_out.to_parquet(output_file, index=False)
    return output_file


def _save_dataframe_output(df, output_folder, service_name, use_parquet):
    if use_parquet:
        return salva_dataframe_parquet(df, output_folder, service_name)
    return salva_dataframe_txt(df, output_folder, service_name)


def _build_filter_url(service_url, filter_expr, select_expr=None):
    if select_expr:
        return f"{service_url}?$filter={filter_expr}&$select={select_expr}"
    return f"{service_url}?$filter={filter_expr}"


def _fetch_dataset(url, bc_type, username, password, tenant_id, client_id, client_secret):
    if bc_type == "SAAS":
        return fetch_data_from_bc_saas(tenant_id, client_id, client_secret, url)
    return fetch_data(url, 'inc', username, password)


def _fetch_keys_snapshot(service_url, bc_type, username, password, tenant_id, client_id, client_secret):
    url = f"{service_url}?$select=SystemId,SystemModifiedAt"
    all_data = _fetch_dataset(url, bc_type, username, password, tenant_id, client_id, client_secret)
    if not all_data:
        return pd.DataFrame(columns=['SystemId', 'SystemModifiedAt'])

    df = pd.DataFrame(all_data)
    for col in ['SystemId', 'SystemModifiedAt']:
        if col not in df.columns:
            df[col] = pd.NA
    return df[['SystemId', 'SystemModifiedAt']].dropna(subset=['SystemId']).drop_duplicates(subset=['SystemId'])


def _fetch_ids_snapshot(service_url, bc_type, username, password, tenant_id, client_id, client_secret):
    url = f"{service_url}?$select=SystemId"
    all_data = _fetch_dataset(url, bc_type, username, password, tenant_id, client_id, client_secret)
    if not all_data:
        return pd.DataFrame(columns=['SystemId'])

    df = pd.DataFrame(all_data)
    if 'SystemId' not in df.columns:
        df['SystemId'] = pd.NA
    return df[['SystemId']].dropna(subset=['SystemId']).drop_duplicates(subset=['SystemId'])


def _fetch_id_set(service_url, bc_type, username, password, tenant_id, client_id, client_secret):
    url = f"{service_url}?$select=SystemId"
    all_data = _fetch_dataset(url, bc_type, username, password, tenant_id, client_id, client_secret)
    if not all_data:
        return set()
    ids = set()
    for row in all_data:
        system_id = row.get('SystemId')
        if system_id:
            ids.add(str(system_id))
    return ids


def _fetch_keys_snapshot_month(service_url, col_rif, year, month,
                               bc_type, username, password, tenant_id, client_id, client_secret):
    start_date = datetime(year, month, 1)
    if month == 12:
        end_date = datetime(year + 1, 1, 1)
    else:
        end_date = datetime(year, month + 1, 1)
    start_str = start_date.strftime('%Y-%m-%dT00:00:00Z')
    end_str = end_date.strftime('%Y-%m-%dT00:00:00Z')
    url = (f"{service_url}?$filter={col_rif} ge {start_str} and {col_rif} lt {end_str}"
           f"&$select=SystemId,SystemModifiedAt")
    all_data = _fetch_dataset(url, bc_type, username, password, tenant_id, client_id, client_secret)
    if not all_data:
        return pd.DataFrame(columns=['SystemId', 'SystemModifiedAt'])
    df = pd.DataFrame(all_data)
    for c in ['SystemId', 'SystemModifiedAt']:
        if c not in df.columns:
            df[c] = pd.NA
    return df[['SystemId', 'SystemModifiedAt']].dropna(subset=['SystemId']).drop_duplicates(subset=['SystemId'])


def _load_txt_dataframe(txt_path):
    if not os.path.exists(txt_path) or os.path.getsize(txt_path) == 0:
        return pd.DataFrame()
    return pd.read_csv(txt_path, sep='\t', dtype=str)


def _load_parquet_dataframe(parquet_path):
    if not os.path.exists(parquet_path) or os.path.getsize(parquet_path) == 0:
        return pd.DataFrame()
    try:
        return pd.read_parquet(parquet_path)
    except Exception:
        return pd.DataFrame()


def _load_keys_snapshot(keys_path):
    candidate_paths = [keys_path]
    if keys_path.endswith('.parquet'):
        candidate_paths.append(keys_path[:-8] + '.txt')
    elif keys_path.endswith('.txt'):
        candidate_paths.append(keys_path[:-4] + '.parquet')

    existing_path = None
    for path in candidate_paths:
        if os.path.exists(path) and os.path.getsize(path) > 0:
            existing_path = path
            break

    if existing_path is None:
        return pd.DataFrame(columns=['SystemId', 'SystemModifiedAt'])

    if existing_path.endswith('.parquet'):
        df = pd.read_parquet(existing_path)
    else:
        df = pd.read_csv(existing_path, sep='\t', dtype=str)

    for col in ['SystemId', 'SystemModifiedAt']:
        if col not in df.columns:
            df[col] = pd.NA
    return df[['SystemId', 'SystemModifiedAt']].dropna(subset=['SystemId']).drop_duplicates(subset=['SystemId'])


def _save_keys_snapshot(df_keys, keys_path):
    os.makedirs(os.path.dirname(keys_path), exist_ok=True)
    if df_keys is None or df_keys.empty:
        if keys_path.endswith('.parquet'):
            pd.DataFrame(columns=['SystemId', 'SystemModifiedAt']).to_parquet(keys_path, index=False)
        else:
            with open(keys_path, 'w', encoding='utf-8'):
                pass
        return
    if keys_path.endswith('.parquet'):
        df_keys.to_parquet(keys_path, index=False)
    else:
        df_keys.to_csv(keys_path, sep='\t', index=False, encoding='utf-8')


def _keys_snapshot_path(output_folder, service_name):
    extension = 'parquet' if use_parquet_keys else 'txt'
    return os.path.join(output_folder, f"{service_name}__keys.{extension}")


def _month_keys_snapshot_path(output_folder, service_name, year, month):
    extension = 'parquet' if use_parquet_keys else 'txt'
    return os.path.join(output_folder, f"{service_name}__keys_{year}_{month:02d}.{extension}")


def _load_partitioned_state(state_path):
    if not os.path.exists(state_path) or os.path.getsize(state_path) == 0:
        return {'initialized': False, 'run_count': 0}
    try:
        with open(state_path, 'r', encoding='utf-8') as f:
            data = json.load(f)
        if isinstance(data, dict):
            return data
    except Exception:
        pass
    return {'initialized': False, 'run_count': 0}


def _save_partitioned_state(state, state_path):
    os.makedirs(os.path.dirname(state_path), exist_ok=True)
    with open(state_path, 'w', encoding='utf-8') as f:
        json.dump(state, f, ensure_ascii=True, indent=2)


def _build_recent_months_to_process(months_back, start_year, end_year, mese_corrente):
    all_months = [
        (y, m)
        for y in range(start_year, end_year + 1)
        for m in (range(1, mese_corrente + 1) if y == end_year else range(1, 13))
    ]
    if months_back <= 0:
        return all_months

    now = datetime.utcnow()
    y = now.year
    m = now.month
    recent = []
    for _ in range(months_back):
        if y < start_year:
            break
        if y == end_year and m > mese_corrente:
            pass
        elif y <= end_year:
            recent.append((y, m))
        m -= 1
        if m == 0:
            m = 12
            y -= 1

    recent_set = set(recent)
    return [item for item in all_months if item in recent_set]


def _write_changed_records_log(output_folder, service_name, rows):
    log_path = os.path.join(output_folder, f"{service_name}__changes_last_run.txt")
    os.makedirs(output_folder, exist_ok=True)
    fieldnames = ['Action', 'SystemId', 'Year', 'Month', 'PrevSystemModifiedAt', 'CurrSystemModifiedAt']
    with open(log_path, 'w', encoding='utf-8', newline='') as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames, delimiter='\t', extrasaction='ignore')
        writer.writeheader()
        for row in rows:
            writer.writerow({k: row.get(k, '') for k in fieldnames})
    return log_path


def _fetch_full_rows_by_ids(service_url, ids, bc_type, username, password, tenant_id, client_id, client_secret, batch_size):
    if not ids:
        return pd.DataFrame()

    batches = [ids[i:i + batch_size] for i in range(0, len(ids), batch_size)]
    df_total = pd.DataFrame()

    def fetch_batch(batch):
        id_filters = " or ".join([f"SystemId eq {sid}" for sid in batch])
        url = _build_filter_url(service_url, id_filters)
        all_data = _fetch_dataset(url, bc_type, username, password, tenant_id, client_id, client_secret)
        return pd.DataFrame(all_data) if all_data else pd.DataFrame()

    workers = max(1, min(incremental_batch_workers, len(batches)))
    with ThreadPoolExecutor(max_workers=workers) as executor:
        futures = [executor.submit(fetch_batch, batch) for batch in batches]
        for future in as_completed(futures):
            df_batch = future.result()
            if not df_batch.empty:
                df_total = pd.concat([df_total, df_batch], ignore_index=True)
    return df_total


def _load_watermark(watermark_path):
    if not os.path.exists(watermark_path) or os.path.getsize(watermark_path) == 0:
        return None
    with open(watermark_path, 'r', encoding='utf-8') as f:
        ts = f.read().strip()
    return ts if ts else None


def _save_watermark(ts, watermark_path):
    os.makedirs(os.path.dirname(watermark_path), exist_ok=True)
    with open(watermark_path, 'w', encoding='utf-8') as f:
        f.write(ts)


def _fetch_since_watermark(service_url, watermark_ts, overlap_minutes,
                           bc_type, username, password, tenant_id, client_id, client_secret):
    try:
        dt = datetime.strptime(watermark_ts[:19].replace(' ', 'T'), '%Y-%m-%dT%H:%M:%S')
    except ValueError:
        dt = datetime.utcnow() - timedelta(hours=1)
    dt_with_overlap = dt - timedelta(minutes=overlap_minutes)
    filter_ts = dt_with_overlap.strftime('%Y-%m-%dT%H:%M:%SZ')
    # Alcuni endpoint BC (es. GLEntry) non supportano $orderby su SystemModifiedAt e rispondono 501.
    url = f"{service_url}?$filter=SystemModifiedAt ge {filter_ts}"
    return _fetch_dataset(url, bc_type, username, password, tenant_id, client_id, client_secret)


def _fetch_deleted_ids_from_changelog(base_url, watermark_ts, overlap_minutes,
                                      bc_type, username, password, tenant_id, client_id, client_secret,
                                      changelog_service_name, changelog_systemid_field, changelog_datetime_field):
    try:
        dt = datetime.strptime(watermark_ts[:19].replace(' ', 'T'), '%Y-%m-%dT%H:%M:%S')
    except ValueError:
        dt = datetime.utcnow() - timedelta(hours=1)

    dt_with_overlap = dt - timedelta(minutes=overlap_minutes)
    filter_ts = dt_with_overlap.strftime('%Y-%m-%dT%H:%M:%SZ')

    changelog_url = build_service_url(base_url, changelog_service_name)
    url = (
        f"{changelog_url}?$filter={changelog_datetime_field} ge {filter_ts}"
        f"&$select={changelog_systemid_field},{changelog_datetime_field}"
    )
    all_data = _fetch_dataset(url, bc_type, username, password, tenant_id, client_id, client_secret)
    if not all_data:
        return set()

    deleted_ids = set()
    for row in all_data:
        system_id = row.get(changelog_systemid_field)
        if system_id:
            deleted_ids.add(str(system_id))
    return deleted_ids


def _upsert_by_system_id(df_base, df_delta):
    if df_delta is None or df_delta.empty:
        return df_base
    if 'SystemId' not in df_delta.columns:
        return df_base

    if df_base is None or df_base.empty:
        return df_delta.drop_duplicates(subset=['SystemId'], keep='last').reset_index(drop=True)

    if 'SystemId' not in df_base.columns:
        # Se il file storico non ha SystemId, lo sostituiamo con il delta consistente.
        return df_delta.drop_duplicates(subset=['SystemId'], keep='last').reset_index(drop=True)

    delta_ids = set(df_delta['SystemId'].dropna().astype(str))
    base = df_base.copy()
    base['SystemId'] = base['SystemId'].astype(str)
    delta = df_delta.copy()
    delta['SystemId'] = delta['SystemId'].astype(str)

    base_filtered = base[~base['SystemId'].isin(delta_ids)]
    out = pd.concat([base_filtered, delta], ignore_index=True)
    return out.drop_duplicates(subset=['SystemId'], keep='last').reset_index(drop=True)


def _merge_incremental_txt_file(txt_path, df_delta, deleted_ids):
    deleted_ids = {str(item) for item in deleted_ids if pd.notna(item)}

    # Fast path: nessun delta e nessun delete -> evita riscrittura completa del file.
    if (df_delta is None or df_delta.empty or 'SystemId' not in df_delta.columns) and not deleted_ids:
        if not os.path.exists(txt_path) or os.path.getsize(txt_path) == 0:
            return 0
        with open(txt_path, 'r', encoding='utf-8', newline='') as src:
            line_count = sum(1 for _ in src)
        return max(0, line_count - 1)

    if df_delta is None or df_delta.empty or 'SystemId' not in df_delta.columns:
        if not os.path.exists(txt_path) or os.path.getsize(txt_path) == 0:
            return 0

        temp_path = f"{txt_path}.tmp"
        row_count = 0
        with open(txt_path, 'r', encoding='utf-8', newline='') as src:
            reader = csv.DictReader(src, delimiter='\t')
            fieldnames = reader.fieldnames or []
            with open(temp_path, 'w', encoding='utf-8', newline='') as dst:
                writer = csv.DictWriter(dst, fieldnames=fieldnames, delimiter='\t', extrasaction='ignore')
                writer.writeheader()
                for row in reader:
                    system_id = str(row.get('SystemId', ''))
                    if system_id and system_id in deleted_ids:
                        continue
                    writer.writerow(row)
                    row_count += 1
        os.replace(temp_path, txt_path)
        return row_count

    delta_df = df_delta.copy()
    delta_df['SystemId'] = delta_df['SystemId'].astype(str)
    delta_rows = delta_df.to_dict('records')
    delta_map = {str(row.get('SystemId', '')): row for row in delta_rows if str(row.get('SystemId', ''))}
    delta_ids = set(delta_map.keys())

    base_fieldnames = []
    if os.path.exists(txt_path) and os.path.getsize(txt_path) > 0:
        with open(txt_path, 'r', encoding='utf-8', newline='') as src:
            reader = csv.DictReader(src, delimiter='\t')
            base_fieldnames = reader.fieldnames or []

    fieldnames = list(base_fieldnames)
    for column in delta_df.columns:
        if column not in fieldnames:
            fieldnames.append(column)
    if not fieldnames:
        fieldnames = list(delta_df.columns)

    temp_path = f"{txt_path}.tmp"
    row_count = 0

    # Percorso veloce su file grandi: merge chunked con pandas (molto piu' rapido del DictReader riga-per-riga).
    first_write = True
    if os.path.exists(txt_path) and os.path.getsize(txt_path) > 0:
        try:
            for chunk in pd.read_csv(txt_path, sep='\t', dtype=str, chunksize=100000):
                if 'SystemId' not in chunk.columns:
                    chunk['SystemId'] = ''
                chunk['SystemId'] = chunk['SystemId'].fillna('').astype(str)

                mask_keep = ~chunk['SystemId'].isin(deleted_ids | delta_ids)
                chunk = chunk.loc[mask_keep].copy()

                for col in fieldnames:
                    if col not in chunk.columns:
                        chunk[col] = ''
                chunk = chunk[fieldnames].fillna('')

                if not chunk.empty:
                    chunk.to_csv(
                        temp_path,
                        sep='\t',
                        index=False,
                        encoding='utf-8',
                        mode='w' if first_write else 'a',
                        header=first_write,
                    )
                    row_count += len(chunk)
                    first_write = False
        except Exception:
            # Fallback robusto alla logica CSV originale in caso di formati imprevisti.
            with open(temp_path, 'w', encoding='utf-8', newline='') as dst:
                writer = csv.DictWriter(dst, fieldnames=fieldnames, delimiter='\t', extrasaction='ignore')
                writer.writeheader()
                with open(txt_path, 'r', encoding='utf-8', newline='') as src:
                    reader = csv.DictReader(src, delimiter='\t')
                    for row in reader:
                        system_id = str(row.get('SystemId', ''))
                        if system_id and (system_id in deleted_ids or system_id in delta_ids):
                            continue
                        normalized_row = {column: row.get(column, '') for column in fieldnames}
                        writer.writerow(normalized_row)
                        row_count += 1
            first_write = False

    # Se non abbiamo ancora scritto nulla, crea almeno l'header.
    if first_write:
        pd.DataFrame(columns=fieldnames).to_csv(
            temp_path,
            sep='\t',
            index=False,
            encoding='utf-8',
            mode='w',
            header=True,
        )
        first_write = False

    delta_out_rows = []
    for system_id, row in delta_map.items():
        if system_id in deleted_ids:
            continue
        delta_out_rows.append({column: row.get(column, '') for column in fieldnames})

    if delta_out_rows:
        delta_df_out = pd.DataFrame(delta_out_rows, columns=fieldnames).fillna('')
        delta_df_out.to_csv(
            temp_path,
            sep='\t',
            index=False,
            encoding='utf-8',
            mode='a',
            header=False,
        )
        row_count += len(delta_df_out)

    os.replace(temp_path, txt_path)
    return row_count


def _merge_incremental_parquet_file(parquet_path, df_delta, deleted_ids):
    deleted_ids = {str(item) for item in deleted_ids if pd.notna(item)}

    has_delta = df_delta is not None and (not df_delta.empty) and ('SystemId' in df_delta.columns)
    if not has_delta and not deleted_ids:
        df_existing = _load_parquet_dataframe(parquet_path)
        return len(df_existing)

    df_base = _load_parquet_dataframe(parquet_path)
    if not df_base.empty and 'SystemId' in df_base.columns:
        df_base['SystemId'] = df_base['SystemId'].astype(str)
        if deleted_ids:
            df_base = df_base[~df_base['SystemId'].isin(deleted_ids)].reset_index(drop=True)

    if has_delta:
        df_work = df_delta.copy()
        df_work['SystemId'] = df_work['SystemId'].astype(str)
        if deleted_ids:
            df_work = df_work[~df_work['SystemId'].isin(deleted_ids)].reset_index(drop=True)
        df_out = _upsert_by_system_id(df_base, df_work)
    else:
        df_out = df_base

    os.makedirs(os.path.dirname(parquet_path), exist_ok=True)
    df_out.to_parquet(parquet_path, index=False)
    return len(df_out)


def _merge_incremental_output_file(path, df_delta, deleted_ids, use_parquet):
    if use_parquet:
        return _merge_incremental_parquet_file(path, df_delta, deleted_ids)
    return _merge_incremental_txt_file(path, df_delta, deleted_ids)


def _write_incremental_report_html(company_name, company_sanitized, rows, output_path):
    report_file = os.path.join(output_path, f"incremental-report-{company_sanitized}.html")

    if not rows:
        body = f"""
        <html>
        <head><meta charset='utf-8'><style>
            body {{ font-family: Arial, sans-serif; margin: 24px; color: #222; }}
            h2 {{ margin-bottom: 8px; }}
            p {{ color: #666; }}
        </style></head>
        <body>
            <h2>Incremental report - {html.escape(company_name)}</h2>
            <p>Nessuna tabella incrementale elaborata in questo run.</p>
        </body>
        </html>
        """
    else:
        table_rows = []
        for row in rows:
            table_rows.append(
                "<tr>"
                f"<td>{html.escape(str(row.get('Service', '')))}</td>"
                f"<td>{html.escape(str(row.get('Mode', '')))}</td>"
                f"<td>{html.escape(str(row.get('Inserted', 0)))}</td>"
                f"<td>{html.escape(str(row.get('Updated', 0)))}</td>"
                f"<td>{html.escape(str(row.get('Deleted', 0)))}</td>"
                f"<td>{html.escape(str(row.get('Changed', 0)))}</td>"
                f"<td>{html.escape(str(row.get('FinalRows', 0)))}</td>"
                f"<td>{html.escape(str(row.get('Details', '')))}</td>"
                "</tr>"
            )

        body = f"""
        <html>
        <head>
            <meta charset='utf-8'>
            <style>
                body {{ font-family: Arial, sans-serif; margin: 24px; color: #222; }}
                h2 {{ margin-bottom: 4px; }}
                .meta {{ color: #666; margin-bottom: 16px; }}
                table {{ width: 100%; border-collapse: collapse; }}
                th, td {{ border: 1px solid #ddd; padding: 8px; font-size: 13px; text-align: left; }}
                th {{ background: #f3f4f6; }}
                tr:nth-child(even) {{ background: #fafafa; }}
            </style>
        </head>
        <body>
            <h2>Incremental report - {html.escape(company_name)}</h2>
            <div class="meta">Generato il {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}</div>
            <table>
                <thead>
                    <tr>
                        <th>Service</th>
                        <th>Mode</th>
                        <th>Insert</th>
                        <th>Update</th>
                        <th>Delete</th>
                        <th>Changed</th>
                        <th>Final rows</th>
                        <th>Details</th>
                    </tr>
                </thead>
                <tbody>
                    {''.join(table_rows)}
                </tbody>
            </table>
        </body>
        </html>
        """

    with open(report_file, 'w', encoding='utf-8') as f:
        f.write(body)
    return report_file


def export_single_service_txt(service_name, company_sanitized, base_url, bc_type, username, password,
                              tenant_id, client_id, client_secret, local_output_path, col_rif=None):
    table_folder = f"{service_name.replace('Qry', '').replace('_', '').lower()}-{company_sanitized}"
    output_folder = os.path.join(local_output_path, table_folder)
    service_url = build_service_url(base_url, service_name)
    txt_path = os.path.join(output_folder, f"{service_name}.txt")
    parquet_path = os.path.join(output_folder, f"{service_name}.parquet")
    data_path = parquet_path if use_parquet_incremental else txt_path
    keys_path = _keys_snapshot_path(output_folder, service_name)
    report_info = {
        'Service': service_name,
        'Mode': 'full',
        'Inserted': 0,
        'Updated': 0,
        'Deleted': 0,
        'Changed': 0,
        'FinalRows': 0,
        'Details': ''
    }

    use_incremental_compare = incremental_compare_enabled and (service_name in incremental_compare_services)

    if use_incremental_compare:
        if incremental_mode == 'guaranteed_fast':
            report_info['Mode'] = 'guaranteed_fast'
            watermark_path = os.path.join(output_folder, f"{service_name}__watermark.txt")
            watermark_ts = _load_watermark(watermark_path)
            data_has_rows = os.path.exists(data_path) and os.path.getsize(data_path) > 0

            prev_keys = _load_keys_snapshot(keys_path)
            prev_keys = prev_keys[['SystemId', 'SystemModifiedAt']] if not prev_keys.empty else pd.DataFrame(columns=['SystemId', 'SystemModifiedAt'])
            prev_ids_set = set(prev_keys['SystemId'].astype(str).tolist()) if not prev_keys.empty else set()

            # Evita di rieseguire full load quando il watermark esiste gia':
            # in quel caso continuiamo in incremental anche se txt/keys sono vuoti.
            first_run = watermark_ts is None
            if first_run:
                all_data = _fetch_dataset(service_url, bc_type, username, password, tenant_id, client_id, client_secret)
                df_out = pd.DataFrame(all_data) if all_data else pd.DataFrame()
                final_txt = _save_dataframe_output(df_out, output_folder, service_name, use_parquet_incremental)

                if not df_out.empty and 'SystemId' in df_out.columns:
                    df_keys_out = df_out[['SystemId', 'SystemModifiedAt']].copy() if 'SystemModifiedAt' in df_out.columns else df_out[['SystemId']].copy()
                    if 'SystemModifiedAt' not in df_keys_out.columns:
                        df_keys_out['SystemModifiedAt'] = pd.NA
                    _save_keys_snapshot(df_keys_out, keys_path)
                else:
                    _save_keys_snapshot(pd.DataFrame(columns=['SystemId', 'SystemModifiedAt']), keys_path)

                new_watermark = datetime.utcnow().strftime('%Y-%m-%dT%H:%M:%SZ')
                if not df_out.empty and 'SystemModifiedAt' in df_out.columns:
                    max_ts = df_out['SystemModifiedAt'].dropna().max()
                    if max_ts:
                        new_watermark = str(max_ts)[:19].replace(' ', 'T') + 'Z'
                _save_watermark(new_watermark, watermark_path)

                report_info['Mode'] = 'initial-full'
                report_info['FinalRows'] = len(df_out)
                report_info['Details'] = 'Primo caricamento completo per inizializzare snapshot chiavi + watermark'
                return service_name, len(df_out), final_txt, report_info

            # Autoripristino: se esiste watermark ma txt/keys sono vuoti o mancanti,
            # ricostruiamo il baseline corrente per evitare cancellazioni/sovrascritture incoerenti.
            if (not data_has_rows) or prev_keys.empty:
                all_data = _fetch_dataset(service_url, bc_type, username, password, tenant_id, client_id, client_secret)
                df_out = pd.DataFrame(all_data) if all_data else pd.DataFrame()
                final_txt = _save_dataframe_output(df_out, output_folder, service_name, use_parquet_incremental)

                if not df_out.empty and 'SystemId' in df_out.columns:
                    df_keys_out = df_out[['SystemId', 'SystemModifiedAt']].copy() if 'SystemModifiedAt' in df_out.columns else df_out[['SystemId']].copy()
                    if 'SystemModifiedAt' not in df_keys_out.columns:
                        df_keys_out['SystemModifiedAt'] = pd.NA
                    _save_keys_snapshot(df_keys_out, keys_path)
                else:
                    _save_keys_snapshot(pd.DataFrame(columns=['SystemId', 'SystemModifiedAt']), keys_path)

                new_watermark = datetime.utcnow().strftime('%Y-%m-%dT%H:%M:%SZ')
                if not df_out.empty and 'SystemModifiedAt' in df_out.columns:
                    max_ts = df_out['SystemModifiedAt'].dropna().max()
                    if max_ts:
                        new_watermark = str(max_ts)[:19].replace(' ', 'T') + 'Z'
                _save_watermark(new_watermark, watermark_path)

                report_info['Mode'] = 'rebuild-full'
                report_info['FinalRows'] = len(df_out)
                report_info['Details'] = 'Ricostruzione baseline: txt/keys vuoti con watermark presente'
                return service_name, len(df_out), final_txt, report_info

            if not watermark_ts:
                watermark_ts = datetime.utcnow().strftime('%Y-%m-%dT00:00:00Z')

            changed_data = _fetch_since_watermark(
                service_url,
                watermark_ts,
                incremental_watermark_overlap_minutes,
                bc_type,
                username,
                password,
                tenant_id,
                client_id,
                client_secret
            )
            df_delta = pd.DataFrame(changed_data) if changed_data else pd.DataFrame()

            if not df_delta.empty and 'SystemId' in df_delta.columns:
                df_delta['SystemId'] = df_delta['SystemId'].astype(str)
                df_delta = df_delta.drop_duplicates(subset=['SystemId'], keep='last')
            else:
                df_delta = pd.DataFrame(columns=['SystemId', 'SystemModifiedAt'])

            delta_ids = set(df_delta['SystemId'].astype(str).tolist()) if not df_delta.empty and 'SystemId' in df_delta.columns else set()
            inserted_ids = delta_ids - prev_ids_set
            updated_ids = delta_ids & prev_ids_set

            delete_mode = 'snapshot'
            if service_name in incremental_changelog_delete_services:
                deleted_ids = _fetch_deleted_ids_from_changelog(
                    base_url,
                    watermark_ts,
                    incremental_watermark_overlap_minutes,
                    bc_type,
                    username,
                    password,
                    tenant_id,
                    client_id,
                    client_secret,
                    incremental_changelog_service_name,
                    incremental_changelog_systemid_field,
                    incremental_changelog_datetime_field
                )
                # Applica delete solo agli ID gia' presenti nello snapshot locale.
                deleted_ids = deleted_ids & prev_ids_set
                delete_mode = 'changelog'
            elif service_name not in incremental_skip_delete_scan_services:
                curr_ids_set = _fetch_id_set(service_url, bc_type, username, password, tenant_id, client_id, client_secret)
                deleted_ids = prev_ids_set - curr_ids_set
                delete_mode = 'snapshot'
            else:
                deleted_ids = set()
                delete_mode = 'none'

            no_changes = (not delta_ids) and (not deleted_ids)
            if no_changes:
                changes_log_path = _write_changed_records_log(output_folder, service_name, [])
                report_info['Inserted'] = 0
                report_info['Updated'] = 0
                report_info['Deleted'] = 0
                report_info['Changed'] = 0
                report_info['FinalRows'] = len(prev_ids_set)
                if delete_mode == 'changelog':
                    report_info['Details'] = (
                        f"Guaranteed-fast: nessuna variazione rilevata (watermark + {incremental_changelog_service_name}); "
                        f"dettagli in {os.path.basename(changes_log_path)}"
                    )
                elif delete_mode == 'none':
                    report_info['Details'] = (
                        f"Guaranteed-fast (no-delete-scan): nessuna variazione rilevata; "
                        f"dettagli in {os.path.basename(changes_log_path)}"
                    )
                else:
                    report_info['Details'] = (
                        f"Guaranteed-fast: nessuna variazione rilevata (watermark + snapshot delete); "
                        f"dettagli in {os.path.basename(changes_log_path)}"
                    )
                return service_name, len(prev_ids_set), data_path, report_info

            final_rows = _merge_incremental_output_file(data_path, df_delta, deleted_ids, use_parquet_incremental)

            delta_keys = pd.DataFrame(columns=['SystemId', 'SystemModifiedAt'])
            if not df_delta.empty:
                if 'SystemModifiedAt' not in df_delta.columns:
                    df_delta['SystemModifiedAt'] = pd.NA
                delta_keys = df_delta[['SystemId', 'SystemModifiedAt']].copy()

            merged_keys = _upsert_by_system_id(prev_keys, delta_keys)
            if deleted_ids and not merged_keys.empty:
                merged_keys = merged_keys[~merged_keys['SystemId'].astype(str).isin(deleted_ids)].reset_index(drop=True)
            _save_keys_snapshot(merged_keys, keys_path)

            new_watermark = watermark_ts
            if not df_delta.empty and 'SystemModifiedAt' in df_delta.columns:
                max_ts = df_delta['SystemModifiedAt'].dropna().max()
                if max_ts:
                    new_watermark = str(max_ts)[:19].replace(' ', 'T') + 'Z'
            _save_watermark(new_watermark, watermark_path)

            prev_modif_map = {}
            if not prev_keys.empty:
                prev_modif_map = {
                    str(row['SystemId']): str(row.get('SystemModifiedAt', ''))
                    for _, row in prev_keys.iterrows()
                }

            changed_records_rows = []
            if not df_delta.empty:
                for _, row in df_delta.iterrows():
                    sid = str(row.get('SystemId', ''))
                    curr_mod = str(row.get('SystemModifiedAt', ''))
                    if sid in inserted_ids:
                        changed_records_rows.append({
                            'Action': 'insert',
                            'SystemId': sid,
                            'Year': '',
                            'Month': '',
                            'PrevSystemModifiedAt': '',
                            'CurrSystemModifiedAt': curr_mod
                        })
                    else:
                        changed_records_rows.append({
                            'Action': 'update',
                            'SystemId': sid,
                            'Year': '',
                            'Month': '',
                            'PrevSystemModifiedAt': prev_modif_map.get(sid, ''),
                            'CurrSystemModifiedAt': curr_mod
                        })

            for sid in deleted_ids:
                changed_records_rows.append({
                    'Action': 'delete',
                    'SystemId': sid,
                    'Year': '',
                    'Month': '',
                    'PrevSystemModifiedAt': prev_modif_map.get(sid, ''),
                    'CurrSystemModifiedAt': ''
                })

            changes_log_path = _write_changed_records_log(output_folder, service_name, changed_records_rows)

            report_info['Inserted'] = len(inserted_ids)
            report_info['Updated'] = len(updated_ids)
            report_info['Deleted'] = len(deleted_ids)
            report_info['Changed'] = len(inserted_ids) + len(updated_ids)
            report_info['FinalRows'] = final_rows
            if delete_mode == 'changelog':
                report_info['Details'] = (
                    f"Guaranteed-fast: insert+update da watermark, delete da {incremental_changelog_service_name}; "
                    f"{len(inserted_ids)} insert, {len(updated_ids)} update, {len(deleted_ids)} delete; "
                    f"dettagli in {os.path.basename(changes_log_path)}"
                )
            elif delete_mode == 'none':
                report_info['Details'] = (
                    f"Guaranteed-fast (no-delete-scan): insert+update da watermark; "
                    f"{len(inserted_ids)} insert, {len(updated_ids)} update, delete scan disattivato per {service_name}; "
                    f"dettagli in {os.path.basename(changes_log_path)}"
                )
            else:
                report_info['Details'] = (
                    f"Guaranteed-fast: insert+update da watermark, delete da snapshot SystemId; "
                    f"{len(inserted_ids)} insert, {len(updated_ids)} update, {len(deleted_ids)} delete; "
                    f"dettagli in {os.path.basename(changes_log_path)}"
                )
            return service_name, final_rows, data_path, report_info

        if incremental_mode == 'partitioned' and col_rif:
            report_info['Mode'] = 'partitioned'
            state_path = os.path.join(output_folder, f"{service_name}__partitioned_state.json")
            state = _load_partitioned_state(state_path)
            run_count = int(state.get('run_count', 0))

            if not bool(state.get('initialized', False)):
                snapshot_exists = any(
                    os.path.exists(_month_keys_snapshot_path(output_folder, service_name, y, m))
                    for y in range(start_year, end_year + 1)
                    for m in (range(1, mese_corrente + 1) if y == end_year else range(1, 13))
                )
                if snapshot_exists:
                    state = {
                        'initialized': True,
                        'run_count': max(1, run_count),
                        'last_run_utc': datetime.utcnow().strftime('%Y-%m-%dT%H:%M:%SZ'),
                        'last_scan_type': 'bootstrap-from-snapshots'
                    }
                    _save_partitioned_state(state, state_path)
                    run_count = int(state.get('run_count', 1))

            if not bool(state.get('initialized', False)):
                # Primo run: full load e costruzione degli snapshot mensili.
                all_data = _fetch_dataset(service_url, bc_type, username, password, tenant_id, client_id, client_secret)
                df_out = pd.DataFrame(all_data) if all_data else pd.DataFrame()
                final_txt = _save_dataframe_output(df_out, output_folder, service_name, use_parquet_incremental)
                if not df_out.empty and col_rif in df_out.columns:
                    df_out_ts = df_out.copy()
                    df_out_ts[col_rif] = pd.to_datetime(df_out_ts[col_rif], errors='coerce', utc=True)
                    for y in range(start_year, end_year + 1):
                        months = range(1, mese_corrente + 1) if y == end_year else range(1, 13)
                        for m in months:
                            mask = (df_out_ts[col_rif].dt.year == y) & (df_out_ts[col_rif].dt.month == m)
                            df_m = df_out_ts[mask][['SystemId', 'SystemModifiedAt']].copy() if 'SystemId' in df_out_ts.columns else pd.DataFrame(columns=['SystemId', 'SystemModifiedAt'])
                            month_keys_path = _month_keys_snapshot_path(output_folder, service_name, y, m)
                            _save_keys_snapshot(df_m, month_keys_path)
                _save_partitioned_state(
                    {
                        'initialized': True,
                        'run_count': 1,
                        'last_run_utc': datetime.utcnow().strftime('%Y-%m-%dT%H:%M:%SZ'),
                        'last_scan_type': 'initial-full'
                    },
                    state_path
                )
                report_info['Mode'] = 'initial-full'
                report_info['FinalRows'] = len(df_out)
                report_info['Details'] = 'Primo caricamento completo per inizializzare gli snapshot mensili'
                return service_name, len(df_out), final_txt, report_info

            # Fetch chiavi per mese in parallelo, confronto, accumulo delta
            all_changed_ids = []
            all_deleted_ids = set()
            total_inserted = 0
            total_updated = 0
            total_deleted = 0
            new_month_snapshots = {}
            changed_records_rows = []

            def process_month(y, m):
                curr_m_keys = _fetch_keys_snapshot_month(
                    service_url, col_rif, y, m,
                    bc_type, username, password, tenant_id, client_id, client_secret
                )
                month_keys_path = _month_keys_snapshot_path(output_folder, service_name, y, m)
                prev_m_keys = _load_keys_snapshot(month_keys_path)
                prev_idx = prev_m_keys.set_index('SystemId') if not prev_m_keys.empty else pd.DataFrame(columns=['SystemModifiedAt'])
                curr_idx = curr_m_keys.set_index('SystemId') if not curr_m_keys.empty else pd.DataFrame(columns=['SystemModifiedAt'])
                prev_ids = set(prev_idx.index.tolist())
                curr_ids = set(curr_idx.index.tolist())
                ins = curr_ids - prev_ids
                dels = prev_ids - curr_ids
                common = curr_ids & prev_ids
                upd = {sid for sid in common if str(curr_idx.at[sid, 'SystemModifiedAt']) != str(prev_idx.at[sid, 'SystemModifiedAt'])}
                changed = list(ins | upd)
                ins_rows = [(sid, '', str(curr_idx.at[sid, 'SystemModifiedAt'])) for sid in ins]
                upd_rows = [(sid, str(prev_idx.at[sid, 'SystemModifiedAt']), str(curr_idx.at[sid, 'SystemModifiedAt'])) for sid in upd]
                del_rows = [(sid, str(prev_idx.at[sid, 'SystemModifiedAt']), '') for sid in dels]
                return y, m, changed, dels, len(ins), len(upd), len(dels), curr_m_keys, ins_rows, upd_rows, del_rows

            next_run_count = run_count + 1
            full_reconcile = (
                incremental_partitioned_full_reconcile_every > 0
                and next_run_count % incremental_partitioned_full_reconcile_every == 0
            )
            if full_reconcile:
                months_to_process = _build_recent_months_to_process(
                    0, start_year, end_year, mese_corrente
                )
                scan_label = 'full-reconcile'
            else:
                months_to_process = _build_recent_months_to_process(
                    incremental_partitioned_months_back, start_year, end_year, mese_corrente
                )
                scan_label = f"recent-{incremental_partitioned_months_back}-months"

            if not months_to_process:
                report_info['FinalRows'] = 0
                report_info['Details'] = 'Nessun mese in finestra incrementale da processare'
                return service_name, 0, data_path, report_info

            month_workers = max(1, min(incremental_batch_workers, len(months_to_process)))
            with ThreadPoolExecutor(max_workers=month_workers) as executor:
                futures = [executor.submit(process_month, y, m) for y, m in months_to_process]
                for future in as_completed(futures):
                    y, m, changed, dels, n_ins, n_upd, n_del, curr_snap, ins_rows, upd_rows, del_rows = future.result()
                    all_changed_ids.extend(changed)
                    all_deleted_ids.update(dels)
                    total_inserted += n_ins
                    total_updated += n_upd
                    total_deleted += n_del
                    new_month_snapshots[(y, m)] = curr_snap
                    for sid, prev_ts, curr_ts in ins_rows:
                        changed_records_rows.append({
                            'Action': 'insert',
                            'SystemId': sid,
                            'Year': y,
                            'Month': f"{m:02d}",
                            'PrevSystemModifiedAt': prev_ts,
                            'CurrSystemModifiedAt': curr_ts
                        })
                    for sid, prev_ts, curr_ts in upd_rows:
                        changed_records_rows.append({
                            'Action': 'update',
                            'SystemId': sid,
                            'Year': y,
                            'Month': f"{m:02d}",
                            'PrevSystemModifiedAt': prev_ts,
                            'CurrSystemModifiedAt': curr_ts
                        })
                    for sid, prev_ts, curr_ts in del_rows:
                        changed_records_rows.append({
                            'Action': 'delete',
                            'SystemId': sid,
                            'Year': y,
                            'Month': f"{m:02d}",
                            'PrevSystemModifiedAt': prev_ts,
                            'CurrSystemModifiedAt': curr_ts
                        })

            report_info['Inserted'] = total_inserted
            report_info['Updated'] = total_updated
            report_info['Deleted'] = total_deleted
            report_info['Changed'] = len(all_changed_ids)

            df_delta_full = _fetch_full_rows_by_ids(
                service_url, all_changed_ids,
                bc_type, username, password, tenant_id, client_id, client_secret,
                incremental_batch_size
            )
            final_rows = _merge_incremental_output_file(data_path, df_delta_full, all_deleted_ids, use_parquet_incremental)

            # Salva snapshot mensili aggiornati
            for (y, m), snap in new_month_snapshots.items():
                month_keys_path = _month_keys_snapshot_path(output_folder, service_name, y, m)
                _save_keys_snapshot(snap, month_keys_path)

            changes_log_path = _write_changed_records_log(output_folder, service_name, changed_records_rows)
            _save_partitioned_state(
                {
                    'initialized': True,
                    'run_count': next_run_count,
                    'last_run_utc': datetime.utcnow().strftime('%Y-%m-%dT%H:%M:%SZ'),
                    'last_scan_type': scan_label,
                    'processed_months': len(months_to_process),
                    'changed_rows': len(changed_records_rows)
                },
                state_path
            )

            report_info['FinalRows'] = final_rows
            report_info['Details'] = (
                f"Partitioned ({scan_label}): {len(months_to_process)} mesi; "
                f"{total_inserted} insert, {total_updated} update, {total_deleted} delete; "
                f"{len(all_changed_ids)} record aggiornati; dettagli in {os.path.basename(changes_log_path)}"
            )
            return service_name, final_rows, data_path, report_info

        if incremental_mode == 'watermark':
            report_info['Mode'] = 'watermark'
            watermark_path = os.path.join(output_folder, f"{service_name}__watermark.txt")
            watermark_ts = _load_watermark(watermark_path)

            if watermark_ts is None:
                # Primo run: full load per inizializzare il watermark.
                all_data = _fetch_dataset(service_url, bc_type, username, password, tenant_id, client_id, client_secret)
                df_out = pd.DataFrame(all_data) if all_data else pd.DataFrame()
                final_txt = _save_dataframe_output(df_out, output_folder, service_name, use_parquet_incremental)
                new_watermark = datetime.utcnow().strftime('%Y-%m-%dT%H:%M:%SZ')
                if not df_out.empty and 'SystemModifiedAt' in df_out.columns:
                    max_ts = df_out['SystemModifiedAt'].dropna().max()
                    if max_ts:
                        new_watermark = str(max_ts)[:19].replace(' ', 'T') + 'Z'
                _save_watermark(new_watermark, watermark_path)
                report_info['Mode'] = 'initial-full'
                report_info['FinalRows'] = len(df_out)
                report_info['Details'] = 'Primo caricamento completo per inizializzare il watermark'
                return service_name, len(df_out), final_txt, report_info

            changed_data = _fetch_since_watermark(
                service_url, watermark_ts, incremental_watermark_overlap_minutes,
                bc_type, username, password, tenant_id, client_id, client_secret
            )
            df_delta = pd.DataFrame(changed_data) if changed_data else pd.DataFrame()
            n_changed = len(df_delta)
            report_info['Changed'] = n_changed
            report_info['Inserted'] = n_changed
            report_info['Updated'] = 0
            report_info['Deleted'] = 0

            final_rows = _merge_incremental_output_file(data_path, df_delta, set(), use_parquet_incremental)

            new_watermark = datetime.utcnow().strftime('%Y-%m-%dT%H:%M:%SZ')
            if not df_delta.empty and 'SystemModifiedAt' in df_delta.columns:
                max_ts = df_delta['SystemModifiedAt'].dropna().max()
                if max_ts:
                    new_watermark = str(max_ts)[:19].replace(' ', 'T') + 'Z'
            _save_watermark(new_watermark, watermark_path)

            report_info['FinalRows'] = final_rows
            report_info['Details'] = (
                f"Watermark: {watermark_ts} \u2192 {new_watermark}; "
                f"{n_changed} record (insert+update, delete non rilevati)"
            )
            return service_name, final_rows, data_path, report_info

        # --- modalità snapshot (confronto chiavi completo) ---
        report_info['Mode'] = 'incremental'
        prev_keys = _load_keys_snapshot(keys_path)
        curr_keys = _fetch_keys_snapshot(service_url, bc_type, username, password, tenant_id, client_id, client_secret)

        prev_idx = prev_keys.set_index('SystemId') if not prev_keys.empty else pd.DataFrame(columns=['SystemModifiedAt'])
        curr_idx = curr_keys.set_index('SystemId') if not curr_keys.empty else pd.DataFrame(columns=['SystemModifiedAt'])

        prev_ids = set(prev_idx.index.tolist())
        curr_ids = set(curr_idx.index.tolist())

        inserted_ids = curr_ids - prev_ids
        deleted_ids = prev_ids - curr_ids
        common_ids = curr_ids & prev_ids

        updated_ids = {
            sid for sid in common_ids
            if str(curr_idx.at[sid, 'SystemModifiedAt']) != str(prev_idx.at[sid, 'SystemModifiedAt'])
        }

        changed_ids = list(inserted_ids | updated_ids)
        report_info['Inserted'] = len(inserted_ids)
        report_info['Updated'] = len(updated_ids)
        report_info['Deleted'] = len(deleted_ids)
        report_info['Changed'] = len(changed_ids)

        if prev_keys.empty:
            # Primo giro: nessun "ieri", quindi facciamo full su tutte le colonne.
            all_data = _fetch_dataset(service_url, bc_type, username, password, tenant_id, client_id, client_secret)
            df_out = pd.DataFrame(all_data) if all_data else pd.DataFrame()
            final_txt = _save_dataframe_output(df_out, output_folder, service_name, use_parquet_incremental)
            _save_keys_snapshot(curr_keys, keys_path)
            report_info['Mode'] = 'initial-full'
            report_info['FinalRows'] = len(df_out)
            report_info['Details'] = 'Primo caricamento completo per inizializzare lo snapshot'
            return service_name, len(df_out), final_txt, report_info

        df_delta_full = _fetch_full_rows_by_ids(
            service_url,
            changed_ids,
            bc_type,
            username,
            password,
            tenant_id,
            client_id,
            client_secret,
            incremental_batch_size
        )

        final_rows = _merge_incremental_output_file(data_path, df_delta_full, deleted_ids, use_parquet_incremental)
        _save_keys_snapshot(curr_keys, keys_path)
        report_info['FinalRows'] = final_rows
        report_info['Details'] = f"Delta applicato su {len(changed_ids)} record; esclusi {len(deleted_ids)} record cancellati"
        return service_name, final_rows, data_path, report_info

    if bc_type == "SAAS":
        all_data = fetch_data_from_bc_saas(tenant_id, client_id, client_secret, service_url)
    else:
        all_data = fetch_data(service_url, 'all', username, password)

    df = pd.DataFrame(all_data) if all_data else pd.DataFrame()
    txt_path = _save_dataframe_output(df, output_folder, service_name, use_parquet_incremental)
    report_info['FinalRows'] = len(df)
    report_info['Details'] = 'Export completo non incrementale'
    return service_name, len(df), txt_path, report_info

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

    if export_full_txt:
        print(f"\n--- EXPORT FULL TXT per company: {company_config['name']} ---")
        servizi_mov = defServ[['Service', 'ColRif']].copy()
        servizi_anag = defServ_anag[['Service', 'ColRif']].copy()
        servizi_totali = pd.concat([servizi_mov, servizi_anag], ignore_index=True).drop_duplicates(subset=['Service'])
        incremental_report_rows = []

        workers = max(1, min(full_export_max_workers, len(servizi_totali)))
        with ThreadPoolExecutor(max_workers=workers) as executor:
            future_map = {}
            for _, service_row in servizi_totali.iterrows():
                service_name = service_row['Service']
                service_col_rif = service_row.get('ColRif', '') or ''
                future = executor.submit(
                    export_single_service_txt,
                    service_name,
                    company_sanitized,
                    base_url,
                    BC_type,
                    username,
                    password,
                    tenant_id,
                    client_id,
                    client_secret,
                    local_output_path,
                    service_col_rif
                )
                future_map[future] = service_name

            for future in as_completed(future_map):
                service_name = future_map[future]
                try:
                    service_name_ret, row_count, txt_path, report_info = future.result()
                    print(f"Tabella {service_name_ret}: esportati {row_count} record in {txt_path}")
                    incremental_report_rows.append(report_info)
                except Exception as e:
                    print(f"Errore export tabella {service_name}: {e}")

        report_file = _write_incremental_report_html(company_config['name'], company_sanitized, incremental_report_rows, base_path)
        print(f"Report incrementale salvato in {report_file}")

        print(f"--- Fine EXPORT FULL TXT per company: {company_config['name']} ---\n")
        continue

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

