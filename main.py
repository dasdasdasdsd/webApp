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

app = FastAPI()

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
