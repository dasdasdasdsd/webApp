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
import io
from azure.storage.blob import BlobServiceClient
from fastapi import FastAPI, HTTPException
import pyodbc
import numpy as np
import spacy
from langdetect import detect
from sklearn.cluster import KMeans
from sklearn.model_selection import cross_val_score
from sklearn.metrics import silhouette_score

# Crea l'applicazione FastAPI
app = FastAPI()


@app.get("/process_and_upload")
async def process_and_upload():
    try:
        # Configuration for SharePoint and MSAL
        sharepoint_site = "fondazioneitsincom.sharepoint.com"
        site_path = "/sites/DATA23-252-ProjectWork"
        client_id = "8eb511d1-bbda-4e27-98e3-205df0956ba7"
        client_secret = "eEG8Q~vqa2WmeR2dkyKfLBroLbyZgb6J53CHKaJr"
        tenant_id = "552bed02-4512-450c-858d-84cfe2b4186d"

        # Authority URL and MSAL Scopes
        authority = f"https://login.microsoftonline.com/{tenant_id}"
        scopes = ["https://graph.microsoft.com/.default"]
        app = msal.ConfidentialClientApplication(
            client_id=client_id,
            client_credential=client_secret,
            authority=authority,
        )

        # Acquire access tokena
        result = app.acquire_token_for_client(scopes=scopes)
        if "access_token" not in result:
            raise Exception("Authentication error:", result.get("error_description"))

        access_token = result["access_token"]
        headers = {"Authorization": f"Bearer {access_token}"}
        # Get the unique `site_id` required to access the site's document libraries
        site_url = f"https://graph.microsoft.com/v1.0/sites/{sharepoint_site}:{site_path}"
        site_response = requests.get(site_url, headers=headers)

        if site_response.status_code != 200:
            raise Exception(f"Failed to get site_id: {site_response.status_code} - {site_response.text}")

        site_id = site_response.json()["id"]
        print(f"Site ID: {site_id}")
        # List drives in the site to find the primary document library
        drive_url = f"https://graph.microsoft.com/v1.0/sites/{site_id}/drives"
        drive_response = requests.get(drive_url, headers=headers)

        if drive_response.status_code != 200:
            raise Exception(f"Failed to get drives: {drive_response.status_code} - {drive_response.text}")

        # Display available drives
        drives = drive_response.json()["value"]
        for drive in drives:
            print(f"Drive Name: {drive['name']}, Drive ID: {drive['id']}")
        # Now we list all folders and files in the main drive to locate the file we need
        drive_id = "b!58sEJNrQCUKJAuJeRleKb2rDu9u9gz1In_o2KU-iZ921oQ8--GkgT5Vi2rW5mTyd"  # Update with the actual drive ID if necessary
        root_url = f"https://graph.microsoft.com/v1.0/drives/{drive_id}/root/children"
        root_response = requests.get(root_url, headers=headers)

        if root_response.status_code != 200:
            raise Exception(f"Failed to get drive contents: {root_response.status_code} - {root_response.text}")

        # Display folders and files
        items = root_response.json()["value"]
        for item in items:
            item_type = "Folder" if "folder" in item else "File"
            print(f"{item_type}: {item['name']} - ID: {item['id']}")
        # Assuming we know the `file_id` of the desired file
        file_id = items[0]['id']  # Replace with the actual file ID
        file_url = f"https://graph.microsoft.com/v1.0/drives/{drive_id}/items/{file_id}/content"
        file_response = requests.get(file_url, headers=headers)

        if file_response.status_code == 200:
            # Read the Excel file content
            file_data = io.BytesIO(file_response.content)
            df_pandas = pd.read_excel(file_data, engine='openpyxl')
            blob_connection_string = "DefaultEndpointsProtocol=https;AccountName=dlpw04;AccountKey=fEJyA9TBzpBIcgqZQJiSHmX+oS47nPtCS9D8aTxfyjN8U4YTY4nr7155JajjYwQB/hKkjr856nOg+AStGdtJBg======;EndpointSuffix=core.windows.net"
            blob_service_client = BlobServiceClient.from_connection_string(blob_connection_string)

            # Nome del container
            container_name = "personas"
            container_client = blob_service_client.get_container_client(container_name)
            # Nome del blob e dati del file
            blob_name = "QUESTIONARIO ITS.xlsx"  # Nome con cui vuoi salvare il file nel blob
            excel_data = io.BytesIO()
            with pd.ExcelWriter(excel_data, engine='openpyxl') as writer:
                df_pandas.to_excel(writer, index=False)
            excel_data.seek(0) 
            # Crea il client per il blob specifico
            blob_client = blob_service_client.get_blob_client(container=container_name, blob=blob_name)

            # Carica i dati della variabile nel blob
            blob_client.upload_blob(excel_data, overwrite=True)
            print(f"File caricato con successo come blob '{blob_name}' nel container '{container_name}'.")
        else:
            print(f"File download error: {file_response.status_code} - {file_response.text}")
    except Exception as e:
            raise HTTPException(status_code=500, detail=f"Error processing documents: {str(e)}")

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
    df_cleaned = df_cleaned.drop(columns=['Surname\n'])
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
@app.get("/execute_ML/")
async def execute_full_script():
    try:
        # Carica entrambi i modelli NLP di SpaCy
        nlp_it = spacy.load("it_core_news_md")
        nlp_en = spacy.load("en_core_web_md")

        # Dati di connessione al database Azure
        server = 'serverpw04.database.windows.net'
        database = 'DBPW04'
        username = 'ADMIN04'
        password = 'indiatango$24'
        connection_str = f'DRIVER={{ODBC Driver 17 for SQL Server}};SERVER={server};DATABASE={database};UID={username};PWD={password}'
        conn = pyodbc.connect(connection_str)

        # Estrarre dati dalla tabella 'personas' per analizzare le lettere di presentazione
        query = """
        SELECT p.Email, p.Course, p.City, p.FullAddress, p.SchoolType, p.SchoolName, 
            p.University, p.UniversityCourse, p.Work, p.ITSDiscovery, 
            p.[Transport C], p.[Transport B], p.[Transport T], p.[Transport O], 
            p.Score, p.Goals, p.ITSLoan, p.PresentationLetter, p.Year, p.Difficoulty
        FROM dbo.personas p
        """
        df = pd.read_sql(query, conn)

        # Funzione per estrarre entità specifiche in base alla lingua
        def extract_entities(text):
            if text is None:
                return {
                    "Strumenti_Tecnici": "Non specificato", "Settore_Interesse": "Non specificato", 
                    "Obiettivi_Carriera": "Non specificato", "Esperienze_Lavorative": "Non specificato", 
                    "Competenze_Trasversali": "Non specificato"
                }
            
            # Rileva la lingua
            lang = detect(text)
            nlp = nlp_it if lang == 'it' else nlp_en  # Seleziona il modello giusto

            # Usa il modello NLP per analizzare il testo
            doc = nlp(text)

            # Regex patterns per catturare informazioni specifiche
            strumenti_tecnici_pattern = r"(Python|SQL|Power BI|Azure|Qlik View|Qlik Sense|programmazione|ETL|machine learning|data visualization|database)"
            settore_interesse_pattern = r"(data analysis|data engineering|economia|informatica|statistica|geopolitica|intelligenza artificiale|motorsport)"
            obiettivi_carriera_pattern = r"(diventare [\w\s]+|lavorare nel settore dei big data|analisi dei dati)"
            esperienza_lavorativa_pattern = r"(?:tirocinio|esperienza (?:in|con|presso))\s+([\w\s]+)"
            competenze_trasversali_pattern = r"(problem solving|lavoro in team|gestione del gruppo|comunicazione|attenzione ai dettagli|orientamento agli obiettivi)"
            
            # Estrazione e pulizia dei dati per ogni pattern
            strumenti_tecnici = [match if isinstance(match, str) else match[0] for match in re.findall(strumenti_tecnici_pattern, text, re.IGNORECASE)]
            settore_interesse = [match if isinstance(match, str) else match[0] for match in re.findall(settore_interesse_pattern, text, re.IGNORECASE)]
            obiettivi_carriera = [match if isinstance(match, str) else match[0] for match in re.findall(obiettivi_carriera_pattern, text, re.IGNORECASE)]
            esperienza_lavorativa = [match if isinstance(match, str) else match[0] for match in re.findall(esperienza_lavorativa_pattern, text, re.IGNORECASE)]
            competenze_trasversali = [match if isinstance(match, str) else match[0] for match in re.findall(competenze_trasversali_pattern, text, re.IGNORECASE)]

            # Combina i risultati e assegna valori di default se vuoti
            return {
                "Strumenti_Tecnici": ', '.join(set(filter(None, strumenti_tecnici))) or "Non specificato",
                "Settore_Interesse": ', '.join(set(filter(None, settore_interesse))) or "Non specificato",
                "Obiettivi_Carriera": ', '.join(set(filter(None, obiettivi_carriera))) or "Non specificato",
                "Esperienze_Lavorative": ', '.join(set(filter(None, esperienza_lavorativa))) or "Non specificato",
                "Competenze_Trasversali": ', '.join(set(filter(None, competenze_trasversali))) or "Non specificato"
            }

        # Applica l'estrazione delle entità e crea nuove colonne
        entities = df['PresentationLetter'].apply(extract_entities)
        df['Strumenti_Tecnici'] = entities.apply(lambda x: x['Strumenti_Tecnici'])
        df['Settore_Interesse'] = entities.apply(lambda x: x['Settore_Interesse'])
        df['Obiettivi_Carriera'] = entities.apply(lambda x: x['Obiettivi_Carriera'])
        df['Esperienze_Lavorative'] = entities.apply(lambda x: x['Esperienze_Lavorative'])
        df['Competenze_Trasversali'] = entities.apply(lambda x: x['Competenze_Trasversali'])

        # Mostra tutti i record, inclusi quelli senza lettera di presentazione
        display(df[['Email', 'PresentationLetter', 
                    'Strumenti_Tecnici', 'Settore_Interesse', 'Obiettivi_Carriera', 
                    'Esperienze_Lavorative', 'Competenze_Trasversali', 
                    'Course', 'City', 'FullAddress', 'SchoolType', 'SchoolName', 
                    'University', 'UniversityCourse', 'Work', 'ITSDiscovery', 
                    'Transport C', 'Transport B', 'Transport T', 'Transport O', 
                    'Score', 'Goals', 'ITSLoan', 'Year', 'Difficoulty']])

        # Conferma per procedere con l'inserimento nel database
        proceed = input("Procedere con l'inserimento nel database? (s/n): ").strip().lower()

        if proceed == 's':
            for _, row in df.iterrows():
                insert_query = """
                INSERT INTO FACT_PERSONAS (DimUtenteID, DimCourseID, DimGeoID, DimSchoolID, DimUniversityID, DimWorkID, 
                                        DimDiscoveryID, DimTransportID, Score, Goals, ITSLoan, PresentationLetter, 
                                        Year, Difficoulty, Percorso_ITS, Strumenti_Tecnici, 
                                        Settore_Interesse, Obiettivi_Carriera, Esperienze_Lavorative, 
                                        Competenze_Trasversali)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """
                conn.execute(insert_query, row['DimUtenteID'], row['DimCourseID'], row['DimGeoID'], row['DimSchoolID'], 
                            row['DimUniversityID'], row['DimWorkID'], row['DimDiscoveryID'], row['DimTransportID'], 
                            row['Score'], row['Goals'], row['ITSLoan'], row['PresentationLetter'], row['Year'], 
                            row['Difficoulty'], row['Percorso_ITS'], row['Strumenti_Tecnici'], 
                            row['Settore_Interesse'], row['Obiettivi_Carriera'], row['Esperienze_Lavorative'], 
                            row['Competenze_Trasversali'])

            conn.commit()
            print("Inserimento completato con successo nella tabella FACT_PERSONAS.")
        else:
            print("Inserimento annullato.")


        # Preparazione del clustering
        df_cluster = df[['Strumenti_Tecnici', 'Settore_Interesse', 'Obiettivi_Carriera',
                        'Course', 'SchoolType', 'University', 
                        'Score', 'Goals']]
        df_cluster = pd.get_dummies(df_cluster)

        # Trova il numero ottimale di cluster basato sulla silhouette media
        silhouette_scores = []
        for k in range(2, 11):  # Evita k=1 poiché non ha senso per silhouette
            kmeans = KMeans(n_clusters=k, random_state=42)
            cluster_labels = kmeans.fit_predict(df_cluster)
            silhouette_avg = silhouette_score(df_cluster, cluster_labels)
            silhouette_scores.append((k, silhouette_avg))

        optimal_clusters = max(silhouette_scores, key=lambda x: x[1])[0]
        print(f"Numero ottimale di cluster secondo il metodo silhouette: {optimal_clusters}")

        kmeans = KMeans(n_clusters=optimal_clusters, random_state=42)
        df['Persona'] = kmeans.fit_predict(df_cluster)

        # Creazione del DataFrame per le personas
        df_personas = df[['Email', 'Persona', 'Strumenti_Tecnici', 'Settore_Interesse', 
                        'Obiettivi_Carriera', 'Esperienze_Lavorative', 'Competenze_Trasversali', 
                        'Course', 'City', 'FullAddress', 'SchoolType', 'SchoolName', 
                        'University', 'UniversityCourse', 'Work', 'ITSDiscovery', 
                        'Transport C', 'Transport B', 'Transport T', 'Transport O', 
                        'Score', 'Goals', 'ITSLoan', 'Year', 'Difficoulty']]

        # Elimina e ricrea la tabella PersonasBI
        conn.execute("IF OBJECT_ID('PersonasBI', 'U') IS NOT NULL DROP TABLE PersonasBI;")
        conn.execute("""
            CREATE TABLE PersonasBI (
                Email NVARCHAR(255),
                Persona INT,
                Strumenti_Tecnici NVARCHAR(255),
                Settore_Interesse NVARCHAR(255),
                Obiettivi_Carriera NVARCHAR(255),
                Esperienze_Lavorative NVARCHAR(255),
                Competenze_Trasversali NVARCHAR(255),
                Course NVARCHAR(255),
                City NVARCHAR(255),
                FullAddress NVARCHAR(255),
                SchoolType NVARCHAR(255),
                SchoolName NVARCHAR(255),
                University NVARCHAR(255),
                UniversityCourse NVARCHAR(255),
                Work NVARCHAR(255),
                ITSDiscovery NVARCHAR(255),
                Transport_C INT,
                Transport_B INT,
                Transport_T INT,
                Transport_O INT,
                Score FLOAT,
                Goals NVARCHAR(255),
                ITSLoan NVARCHAR(255),
                Year INT,
                Difficoulty INT
            )
        """)

        # Inserimento dei dati nella tabella PersonasBI
        insert_query = """
            INSERT INTO PersonasBI (Email, Persona, Strumenti_Tecnici, Settore_Interesse, 
                                    Obiettivi_Carriera, Esperienze_Lavorative, Competenze_Trasversali, 
                                    Course, City, FullAddress, SchoolType, SchoolName, 
                                    University, UniversityCourse, Work, ITSDiscovery, 
                                    Transport_C, Transport_B, Transport_T, Transport_O, 
                                    Score, Goals, ITSLoan, Year, Difficoulty)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """

        for _, row in df_personas.iterrows():
            conn.execute(insert_query, (
                row['Email'], row['Persona'], row['Strumenti_Tecnici'], row['Settore_Interesse'],
                row['Obiettivi_Carriera'], row['Esperienze_Lavorative'], row['Competenze_Trasversali'],
                row['Course'], row['City'], row['FullAddress'], row['SchoolType'], row['SchoolName'],
                row['University'], row['UniversityCourse'], row['Work'], row['ITSDiscovery'],
                row['Transport C'], row['Transport B'], row['Transport T'], row['Transport O'],
                row['Score'], row['Goals'], row['ITSLoan'], row['Year'], row['Difficoulty']
            ))
        # Calcola la validazione incrociata (5 fold)
        cross_val_scores = cross_val_score(model, X, y, cv=5, scoring='accuracy')
        cross_val_mean = np.mean(cross_val_scores)  # Media delle accuracy

        # Prepara i dati per l'inserimento nel database
        df_model_evaluation = pd.DataFrame([
            {'Metriche': 'Cross-Validation Accuracy Media', 'Valore': cross_val_mean}
        ] + [
            {'Metriche': f'Cross-Validation Fold {i} Accuracy', 'Valore': score} 
            for i, score in enumerate(cross_val_scores, start=1)
        ])

        # Conferma prima dell'inserimento
        print("Dati di valutazione del modello (con cross-validation):")
        print(df_model_evaluation)
        proceed = input("Procedere con l'inserimento nel database? (s/n): ").strip().lower()

        if proceed == 's':
            # Crea la tabella se non esiste
            conn.execute("""
            IF NOT EXISTS (SELECT * FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME = 'ML_Model_Evaluation')
            BEGIN
                CREATE TABLE ML_Model_Evaluation (
                    Metriche NVARCHAR(50),
                    Valore FLOAT
                );
            END;
            """)
            conn.commit()

            # Inserisci i dati nella tabella
            insert_query = "INSERT INTO ML_Model_Evaluation (Metriche, Valore) VALUES (?, ?)"
            for _, row in df_model_evaluation.iterrows():
                conn.execute(insert_query, row['Metriche'], float(row['Valore']))
            
            conn.commit()
            print("Dati di valutazione inseriti nel database.")
        else:
            print("Inserimento annullato.")

        # Chiudi la connessione
        conn.close()
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error executing script: {str(e)}")

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
