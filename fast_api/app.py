import os ,json, logging

from datetime import datetime
from google.cloud import storage

from fastapi import FastAPI, HTTPException, UploadFile, File, Request, Query
from fastapi.middleware.cors import CORSMiddleware

from utils.auth_utils import call_runner
from utils.gcs_utils import download_from_gcs, upload_config
from utils.config import RESULTS_ENDPOINT, CHAT_ENDPOINT, ANALYZE_ALL_ENDPOINT,\
    N_BATCHES, RESULT_FILENAME, RESULT_PATH, ASSET_BUCKET_NAME, GCS_BATCH_DIR, CLOUD_RUN_URL


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
    allow_origins=["*"],    # TODO: prima di pushare in produzione, da sostituire con URL di API in frontend (compito Samu)
    allow_credentials=True,
    allow_methods=["GET", "POST", "OPTIONS"],
    allow_headers=["Content-Type", "Authorization"],
)

@app.on_event("startup")
async def startup_event():
    upload_config()    # caricamento variabili d'ambiente su GCS
    logging.info("Fast API - API avviata e pronta.")


# --- Endpoints ---------------------------------
@app.get("/")
def read_root():
    return {"status": "running"}


# Check di stato di API e runner
@app.get("/check-status")
async def check_status():
    try:
        data = await call_runner("GET", CLOUD_RUN_URL)
        status = data.get("status", "unknown")

    except Exception as e:
        logging.error(f"[app|check_status] Errore: {e}")
        status = f"errore: {str(e)}"

    return {"API": "running", "runner": status}


# Visualizzazione alert classificati
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


# (TODO: controllare formato di risposta e magari renderla più discorsiva)
@app.post(CHAT_ENDPOINT)
async def chat(request: Request):
    try:
        alert_json = await request.json()
        result = await call_runner(
            method="POST",
            url=f"{CLOUD_RUN_URL}/run-alert",
            json={"alert": alert_json}
        )

        logging.info(f"Alert inviato a Cloud Run")

    except Exception as e:
        logging.error(f"[app|chat] Errore in invio alert: {e}")
        return {"explanation": f"Errore sconosciuto. Ulteriori info nei log locali al server Fast API"}
    
    return {"explanation": result["explanation"]}



# Endpoint che riceve file con dataset, lo divide in N batch per farli poi eseguire in parallelo dal server Cloud Run
@app.post(ANALYZE_ALL_ENDPOINT)
async def analyze_all(file: UploadFile = File(...), n_batches: int = Query(N_BATCHES, gt=0)):
   # Caricamento variabili d'ambiente su GCS
    upload_config(n_batches)

    # Elaborazione dati
    content = await file.read()
    lines = content.decode().splitlines()
    total = len(lines)
    chunk_size = total // n_batches
    run_id = datetime.now().strftime("%Y%m%d-%H%M%S")

    # Connessione al bucket
    bucket = storage.Client().bucket(ASSET_BUCKET_NAME)

    for i in range(n_batches):
        # Suddivisione file in N batch
        start = i * chunk_size
        end = (i + 1) * chunk_size if i < n_batches - 1 else total
        batch_lines = lines[start:end]
        #batch_lines = lines[i*chunk_size:(i+1)*chunk_size] if i < n_batches - 1 else lines[i*chunk_size:]      #(versione compatta)
        
        # Salvataggio file batch in GCS
        batch_path = f"{GCS_BATCH_DIR}/{run_id}-batch-{i}.jsonl"
        blob = bucket.blob(batch_path)
        blob.upload_from_string("\n".join(batch_lines))

        # Invia richiesta HTTP a Cloud Run per analizzare l'i-esimo batch
        try:
            await call_runner(
                method="POST",
                url=f"{CLOUD_RUN_URL}/run-batch",
                json={"batch_file": batch_path}
            )

            logging.info(f"Batch {i} inviato a Cloud Run")

        except Exception as e:
            logging.error(f"[app|analyze_all] Errore in invio batch {i}: {e}")
            return {"message": f"Errore in invio batch {i}. Ulteriori info nei log locali al server Fast API", "run_id": run_id}

    return {"message": f"{n_batches} batch inviati", "run_id": run_id}
