from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
from azure.storage.blob import BlobServiceClient
from azure.core.credentials import AzureKeyCredential
from azure.ai.documentintelligence import DocumentIntelligenceClient
import pandas as pd
import re
from sqlalchemy import create_engine
from nltk.corpus import stopwords
import json
import msal
import requests
import pandas as pd
import io
from azure.storage.blob import BlobServiceClient
from fastapi import FastAPI, HTTPException

app = FastAPI()



# Configurazioni Azure e SharePoint
sharepoint_site = "fondazioneitsincom.sharepoint.com"
site_path = "/sites/DATA23-252-ProjectWork"
client_id = "8eb511d1-bbda-4e27-98e3-205df0956ba7"
client_secret = "eEG8Q~vqa2WmeR2dkyKfLBroLbyZgb6J53CHKaJr"
tenant_id = "552bed02-4512-450c-858d-84cfe2b4186d"
blob_connection_string = "DefaultEndpointsProtocol=https;AccountName=dlpw04;AccountKey=fEJyA9TBzpBIcgqZQJiSHmX+oS47nPtCS9D8aTxfyjN8U4YTY4nr7155JajjYwQB/hKkjr856nOg+AStGdtJBg======;EndpointSuffix=core.windows.net"
container_name = "personas"
drive_id = "b!58sEJNrQCUKJAuJeRleKb2rDu9u9gz1In_o2KU-iZ921oQ8--GkgT5Vi2rW5mTyd"  # Drive ID
blob_name = "QUESTIONARIO_ITS.xlsx"


# 1. Funzione per ottenere il token di accesso (MSAL)
def acquire_access_token(client_id: str, client_secret: str, tenant_id: str) -> str:
    authority = f"https://login.microsoftonline.com/{tenant_id}"
    scopes = ["https://graph.microsoft.com/.default"]
    
    app = msal.ConfidentialClientApplication(
        client_id=client_id,
        client_credential=client_secret,
        authority=authority,
    )
    
    result = app.acquire_token_for_client(scopes=scopes)
    
    if "access_token" not in result:
        raise Exception("Authentication error:", result.get("error_description"))
    
    return result["access_token"]

# 2. Funzione per ottenere l'ID del sito SharePoint
def get_sharepoint_site_id(sharepoint_site: str, site_path: str, access_token: str) -> str:
    site_url = f"https://graph.microsoft.com/v1.0/sites/{sharepoint_site}:{site_path}"
    headers = {"Authorization": f"Bearer {access_token}"}
    
    site_response = requests.get(site_url, headers=headers)
    
    if site_response.status_code != 200:
        raise Exception(f"Failed to get site_id: {site_response.status_code} - {site_response.text}")
    
    return site_response.json()["id"]

# 3. Funzione per ottenere i drives di SharePoint
def list_drives(site_id: str, access_token: str) -> list:
    drive_url = f"https://graph.microsoft.com/v1.0/sites/{site_id}/drives"
    headers = {"Authorization": f"Bearer {access_token}"}
    
    drive_response = requests.get(drive_url, headers=headers)
    
    if drive_response.status_code != 200:
        raise Exception(f"Failed to get drives: {drive_response.status_code} - {drive_response.text}")
    
    return drive_response.json()["value"]

# 4. Funzione per ottenere i contenuti di un drive (file/folder)
def list_drive_contents(drive_id: str, access_token: str) -> list:
    root_url = f"https://graph.microsoft.com/v1.0/drives/{drive_id}/root/children"
    headers = {"Authorization": f"Bearer {access_token}"}
    
    root_response = requests.get(root_url, headers=headers)
    
    if root_response.status_code != 200:
        raise Exception(f"Failed to get drive contents: {root_response.status_code} - {root_response.text}")
    
    return root_response.json()["value"]

# 5. Funzione per scaricare il file da SharePoint
def download_file_from_sharepoint(drive_id: str, file_id: str, access_token: str) -> io.BytesIO:
    file_url = f"https://graph.microsoft.com/v1.0/drives/{drive_id}/items/{file_id}/content"
    headers = {"Authorization": f"Bearer {access_token}"}
    
    file_response = requests.get(file_url, headers=headers)
    
    if file_response.status_code == 200:
        return io.BytesIO(file_response.content)
    else:
        raise Exception(f"File download error: {file_response.status_code} - {file_response.text}")

# 6. Funzione per caricare il file in Azure Blob Storage
def upload_file_to_blob(df_pandas: pd.DataFrame, container_name: str, blob_name: str, connection_string: str) -> None:
    blob_service_client = BlobServiceClient.from_connection_string(connection_string)
    
    # Salva i dati del DataFrame in un file Excel temporaneo in memoria
    excel_data = io.BytesIO()
    with pd.ExcelWriter(excel_data, engine='openpyxl') as writer:
        df_pandas.to_excel(writer, index=False)
    excel_data.seek(0)
    
    # Crea il client per il blob specifico
    blob_client = blob_service_client.get_blob_client(container=container_name, blob=blob_name)
    
    # Carica i dati nel blob
    blob_client.upload_blob(excel_data, overwrite=True)
    print(f"File caricato con successo come blob '{blob_name}' nel container '{container_name}'.")

# 7. Funzione principale che esegue il flusso completo
def process_sharepoint_file_and_upload(client_id: str, client_secret: str, tenant_id: str,
                                       sharepoint_site: str, site_path: str, drive_id: str, 
                                       file_id: str, container_name: str, blob_name: str, 
                                       blob_connection_string: str) -> None:
    # Ottieni il token di accesso
    access_token = acquire_access_token(client_id, client_secret, tenant_id)
    
    # Ottieni l'ID del sito SharePoint
    site_id = get_sharepoint_site_id(sharepoint_site, site_path, access_token)
    
    # Ottieni la lista dei drives
    drives = list_drives(site_id, access_token)
    print("Drives:", drives)
    
    # Ottieni i contenuti del drive
    items = list_drive_contents(drive_id, access_token)
    
    # Scarica il file da SharePoint
    file_data = download_file_from_sharepoint(drive_id, file_id, access_token)
    
    # Carica il file su Azure Blob Storage
    df_pandas = pd.read_excel(file_data, engine='openpyxl')
    upload_file_to_blob(df_pandas, container_name, blob_name, blob_connection_string)

# Endpoint FastAPI che chiama la funzione principale
@app.get("/process_and_upload/")
async def process_and_upload():
    try:
        # Chiamata della funzione principale con i parametri necessari
        process_sharepoint_file_and_upload(
            client_id=client_id,
            client_secret=client_secret,
            tenant_id=tenant_id,
            sharepoint_site=sharepoint_site,
            site_path=site_path,
            drive_id=drive_id,
            file_id="b!58sEJNrQCUKJAuJeRleKb2rDu9u9gz1In_o2KU-iZ921oQ8--GkgT5Vi2rW5mTyd",  # File ID che hai fornito
            container_name=container_name,
            blob_name=blob_name,
            blob_connection_string=blob_connection_string
        )
        return {"message": "File caricato correttamente nel blob."}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error: {str(e)}")

# Azure Document Intelligence and Blob Storage credentials
endpoint = "https://dipw042.cognitiveservices.azure.com/"
key = "4JEgqoVCUcwcdy333RKhIoZUVVyGZscvhdEaa9rB3c4BKJTpB9osJQQJ99AKACYeBjFXJ3w3AAALACOGOd7f"
document_intelligence_client = DocumentIntelligenceClient(endpoint, AzureKeyCredential(key))
blob_connection_string = "DefaultEndpointsProtocol=https;AccountName=dlpw04;AccountKey=fEJyA9TBzpBIcgqZQJiSHmX+oS47nPtCS9D8aTxfyjN8U4YTY4nr7155JajjYwQB/hKkjr856nOg+AStGdtJBg======;EndpointSuffix=core.windows.net"
blob_service_client = BlobServiceClient.from_connection_string(blob_connection_string)
container_name = "test"

# Database connection info
server = 'serverpw04.database.windows.net'
database = 'DBPW04'
username = 'ADMIN04'
password = 'indiatango$24'
driver= 'ODBC+Driver+17+for+SQL+Server'
connection_string = f'mssql+pyodbc://{username}:{password}@{server}:1433/{database}?driver={driver.replace(" ", "+")}'
engine = create_engine(connection_string)

def process_documents():
    results_list = []
    try:
        container_client = blob_service_client.get_container_client(container_name)
        for blob in container_client.list_blobs():
            blob_client = container_client.get_blob_client(blob.name)
            document_bytes = blob_client.download_blob().readall()
            poller = document_intelligence_client.begin_analyze_document(
                model_id="DEFINITIVO",
                analyze_request=document_bytes,
                content_type="application/pdf"
            )
            result = poller.result()
            for document in result.documents:
                document_data = {"doc_type": document.doc_type}
                for name, field in document.fields.items():
                    document_data[f"campo_{name}"] = field.content
                results_list.append(document_data)
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error processing documents: {str(e)}")
    return results_list

def clean_data(results_list):
    df = pd.DataFrame(results_list)
    df['campo_Sesso'] = df.apply(lambda row: 'M' if row['campo_Sesso M'] == ':selected:' else 'F' if row['campo_Sesso F'] == ':selected:' else None, axis=1)
    df = df.drop(columns=['campo_Sesso M', 'campo_Sesso F'])
    campo_its_columns = [col for col in df.columns if col.startswith('campo_ITS')]
    df['corso'] = df.apply(lambda row: next((col for col in campo_its_columns if row[col] == ':selected:'), None), axis=1)
    df = df.drop(columns=campo_its_columns)
    df['campo_Provincia di Residenza'] = df['campo_Provincia di Residenza'].apply(lambda x: re.sub(r'\)', '', x).strip() if isinstance(x, str) else x)
    df['campo_Nome e Cognome'] = df['campo_Nome e Cognome'].str.replace('\n', ' ', regex=True).str.strip()
    df['campo_Nome e Cognome'] = df['campo_Nome e Cognome'].apply(lambda x: ' '.join(reversed(x.split()))).str.title()
    df['campo_Numero di telefono'] = df['campo_Numero di telefono'].str.replace(r' ', '', regex=True).replace('^.*\n', '', regex=True).replace(r'^.*39/', '', regex=True).replace(r'/', '', regex=True).str.strip()
    df['campo_Via'] = df['campo_Via'].str.replace('n.*', '', regex=True).str.strip()
    df.rename(columns={'campo_Nome e Cognome': 'CompleteName', 'campo_Numero di telefono': 'PhoneNumber'}, inplace=True)
    df['FullAddress'] = df['campo_Via'] + ' ' + df['campo_Numero Civico'].astype(str)
    df['FullAddress'] = df['FullAddress'].str.title()
    df = df[['FullAddress', 'PhoneNumber', 'CompleteName']]
    return df

def clean_combined_data():
    df_cleaned = pd.read_sql('SELECT * FROM stg.personas', con=engine)
    df_cleaned['Name'] = df_cleaned['Name'].str.capitalize()
    df_cleaned['Surname'] = df_cleaned['Surname\n'].str.capitalize()
    df_cleaned['City'] = df_cleaned['City'].str.replace(r'\s*\(.*?\)', '', regex=True).replace(r',.*', '', regex=True).str.capitalize()
    df_cleaned['Transports'] = df_cleaned['Transports'].str.rstrip(';')
    transport_columns = df_cleaned['Transports'].str.split(';', expand=True)
    df_cleaned['Transport C'] = transport_columns.apply(lambda x: 1 if 'Car' in x.values else 0, axis=1)
    df_cleaned['Transport B'] = transport_columns.apply(lambda x: 1 if 'Bus' in x.values else 0, axis=1)
    df_cleaned['Transport T'] = transport_columns.apply(lambda x: 1 if 'Train' in x.values else 0, axis=1)
    df_cleaned['Transport O'] = transport_columns.apply(lambda x: 1 if 'Other' in x.values else 0, axis=1)
    df_cleaned = df_cleaned.drop(columns=['Transports'])
    stop_words = set(stopwords.words('italian')).union(set(stopwords.words('english')))
    df_cleaned['Goals'] = df_cleaned['Goals'].apply(lambda x: ' '.join(word for word in str(x).split() if word.lower() not in stop_words))
    df_cleaned['UniversityCourse'] = df_cleaned['UniversityCourse'].str.capitalize()
    df_cleaned['Score'] = df_cleaned['Score'].str.replace(r'/.*', '', regex=True).str.strip()
    return df_cleaned

@app.get("/execute_full_script/")
async def execute_full_script():
    try:
        # Step 1: Process documents from Blob storage
        # results = process_documents()

        # Step 2: Clean the data extracted from documents
        # cleaned_data = clean_data(results)

        # Step 3: Clean combined data from SQL database
        df = clean_combined_data()

        # Step 4: Combine data and save to database
        # df_combined = pd.merge(cleaned_combined_data, cleaned_data, on='CompleteName', how='left')
        df.to_sql('personas', con=engine, if_exists='replace', index=False)

        return {"message": "Script executed successfully and data saved to database."}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error executing script: {str(e)}")
    finally:
        engine.dispose()

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
