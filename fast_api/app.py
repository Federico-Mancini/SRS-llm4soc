import os ,json
import shutil, logging, requests

from datetime import datetime
from google.cloud import storage
from fastapi import FastAPI, HTTPException, UploadFile, File, Request, Query
from fastapi.response import JSONResponse
from fastapi.middleware.cors import CORSMiddleware

from utils.gcs_utils import download_from_gcs, upload_config
from utils.config import RESULTS_ENDPOINT, ANALYZE_ENDPOINT, UPLOAD_ENDPOINT, CHAT_ENDPOINT, ANALYZE_ALL_ENDPOINT,\
    N_BATCHES, RESULT_FILENAME, RESULT_PATH, ALERTS_PATH, ASSET_BUCKET_NAME, GCS_BATCH_DIR, CLOUD_RUN_URL


# --- Logging -----------------------------------
os.makedirs("./assets", exist_ok=True)     # creazione cartella per i log, in caso non esista

logging.basicConfig(
    filename="./assets/server.log",
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S"
)


# --- API configuration -------------------------
app = FastAPI()
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # TODO: in produzione mettere solo il dominio frontend
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


# --- Endpoints ---------------------------------
@app.get("/")
def read_root():
    return {"message": "API LLM4SOC attiva!"}


# Endoint 1.0 - CHECKED
@app.get(RESULTS_ENDPOINT)
def get_results():
    download_from_gcs(RESULT_FILENAME)     # download risultati da GCS
    
    if not os.path.exists(RESULT_PATH):
        logging.warning(f"GET /results | File '{RESULT_FILENAME}' non trovato")
        raise HTTPException(status_code=404, detail=f"File '{RESULT_FILENAME}' non trovato")
    
    try:
        with open(RESULT_PATH, "r") as f:
            data = json.load(f)
        logging.info(f"GET /results | File letto: '{RESULT_FILENAME}'")
        return data
    
    except json.JSONDecodeError:
        logging.error(f"GET /results | Errore parsing '{RESULT_FILENAME}'")
        raise HTTPException(status_code=500, detail=f"Errore nel parsing del file {RESULT_FILENAME}")
    
    except Exception as e:
        logging.error(f"GET /results | Error reading '{RESULT_FILENAME}': {str(e)}")
        raise HTTPException(status_code=500, detail=f"Errore nella lettura del file '{RESULT_FILENAME}': {str(e)}")


# Endpoint 1.0 - TODO: ritrasmettere la richiesta al Cloud Run server
@app.post(ANALYZE_ENDPOINT)
async def analyze_alerts():
    if not os.path.exists(ALERTS_PATH):
        logging.warning("POST /analyze | File da analizzare non trovato")
        raise HTTPException(status_code=404, detail="File da analizzare non trovato")
    
    try:
        results = analyze_batch(ALERTS_PATH)
        logging.info(f"POST /analyze | {len(results)} alert analizzati")

        return JSONResponse(content={
            "message": f"Analisi completata. Risultati visibili anche a '{RESULTS_ENDPOINT}'",
            "num_alerts": len(results),
            "results": results
        })
    
    except Exception as e:
        logging.error(f"POST /analyze | Errore in analisi batch: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Errore nell'analisi degli alert: {str(e)}")


# Endpoint 1.0 - OBSOLETE (comunque può ancora essere usato per memorizzare localmente a Fast API (VM) il dataset)
@app.post(UPLOAD_ENDPOINT)
async def upload_alerts(file: UploadFile = File(...)):
    # Verifica estensione file
    if not file.filename.endswith((".csv", ".json", ".jsonl")):
        logging.warning(f"POST /upload | File non supportato: {file.filename}")
        raise HTTPException(status_code=400, detail="Il file da analizzare deve essere in formato CSV, JSON o JSONL")

    try:
        with open(ALERTS_PATH, "wb") as f:
            shutil.copyfileobj(file.file, f)    # copia dei byte del file ricevuto in locale
        logging.info(f"POST /upload | File caricato: {file.filename}")
        return {"filename": file.filename, "message": "File caricato con successo"}

    except Exception as e:
        logging.error(f"POST /upload | Errore in salvataggio: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Errore nel salvataggio del file: {str(e)}")


# Endpoint 1.0 - CHECKED (TODO: controllare formato di risposta e magari renderla più discorsiva)
@app.post(CHAT_ENDPOINT)
async def chat(request: Request):
    try:
        alert_text = await request.body().decode("utf-8")
        result = analyze_alert(alert_text)
        logging.info(f"POST /chat | Alert analizzato")
        return {"explanation": result.explanation}
    
    except Exception as e:
        logging.error(f"POST /chat | Errore in analisi alert: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Errore nell'analisi dell'alert: {str(e)}")


# Endpoint 2.0 (riceve file con dataset, lo divide in N batch per farli poi eseguire in parallelo dal server Cloud Run)
@app.post(ANALYZE_ALL_ENDPOINT)
async def analyze_all(file: UploadFile = File(...), n_batches: int = Query(..., gt=0)):

    # Elaborazione dati
    content = await file.read()
    lines = content.decode().splitlines()
    total = len(lines)
    chunk_size = total // N_BATCHES
    run_id = datetime.now().strftime("%Y%m%d-%H%M%S")

    # Connessione al bucket
    bucket = storage.Client().bucket(ASSET_BUCKET_NAME)

    # Caricamento variabili d'ambiente su GCS
    upload_config(n_batches)

    for i in range(N_BATCHES):
        # Suddivisione file in N batch
        start = i * chunk_size
        end = (i + 1) * chunk_size if i < N_BATCHES - 1 else total
        batch_lines = lines[start:end]
        #batch_lines = lines[i*chunk_size:(i+1)*chunk_size] if i < N_BATCHES - 1 else lines[i*chunk_size:]      #(versione compatta)
        
        # Salvataggio file batch in GCS
        batch_path = f"{GCS_BATCH_DIR}/{run_id}-batch-{i}.jsonl"
        blob = bucket.blob(batch_path)
        blob.upload_from_string("\n".join(batch_lines))

        # Invia richiesta HTTP a Cloud Run per analizzare l'i-esimo batch
        requests.post(CLOUD_RUN_URL, json={"batch_file": batch_path})

    return {"message": f"{N_BATCHES} batch lanciati", "run_id": run_id}
