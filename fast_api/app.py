# VMS: Virtual Machine Server

import os ,json
import utils.gcs_utils as gcs

from fastapi import FastAPI, HTTPException, UploadFile, File, Request, Query
from fastapi.middleware.cors import CORSMiddleware

from utils.resource_manager import ResourceManager
from utils.auth_utils import call_runner


res = ResourceManager()


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
    gcs.upload_to("", res.vms_config_filename)    # ad ogni avvio carico il file 'config.json' su GCS
    res.logger.info("[VMS][app][startup_event] Virtual Machine Server - Status: running")


# --- Endpoints ---------------------------------
@app.get("/")
def read_root():
    return {"status": "running"}


# Check di stato di API e runner
@app.get("/check-status")
async def check_status():
    try:
        data = await call_runner("GET", res.runner_url)
        status = data.get("status", "unknown")

    except Exception as e:
        res.logger.error(f"[VMS][app][check_status] Unknown error: {e}")
        status = f"errore: {str(e)}"

    return {"API": "running", "runner": status}


# Visualizzazione file con alert classificati
@app.get("/result")
def get_result(dataset_filename: str = Query(...)):
    dataset_name = os.path.splitext(dataset_filename)[0]
    gcs_result_path = f"{res.gcs_result_dir}/{dataset_name}_result.json"
    blob = res.bucket.blob(gcs_result_path)
    
    # Controllo esistenza file remoto
    if not blob.exists():
        res.logger.warning(f"[VMS][app][get_result] File {gcs_result_path} not found")
        raise HTTPException(status_code=404, detail=f"File {gcs_result_path} non trovato")

    blob.download_to_filename(res.vms_result_path)

    # Controllo esito download in locale
    if not os.path.exists(res.vms_result_path):
        res.logger.warning(f"[VMS][app][get_result] Downloaded file {res.vms_result_path} not found")
        raise HTTPException(status_code=404, detail=f"File scaricato {res.vms_result_path} non trovato")
    
    try:
        with open(res.vms_result_path, "r") as f:
            data = json.load(f)

        res.logger.info(f"[VMS][app][get_result] File {res.vms_result_path} read")
        return data
    
    except json.JSONDecodeError:
        res.logger.error(f"[VMS][app][get_result] Failed to parse {res.vms_result_path} ({type(e)}): {str(e)}")
        raise HTTPException(status_code=500, detail=f"Errore nel parsing del file {res.vms_result_path} ({type(e)}): {str(e)}")
    
    except Exception as e:
        res.logger.error(f"[VMS][app][get_result] Failed to read {res.vms_result_path} ({type(e)}): {str(e)}")
        raise HTTPException(status_code=500, detail=f"Errore nella lettura del file {res.vms_result_path} ({type(e)}): {str(e)}")


# (TODO: controllare formato di risposta e magari renderla più discorsiva)
@app.post("/chat")
async def chat(request: Request):
    try:
        alert_json = await request.json()
        
        result = await call_runner(
            method="POST",
            url=f"{res.runner_url}/run-alert",
            json={"alert": alert_json}
        )

        res.logger.info("[VMS][app][chat] Request sent to the runner")

    except Exception as e:
        res.logger.error(f"[VMS][app][chat] Failed to send alert data ({type(e)}): {str(e)}")
        return {"explanation": f"Errore sconosciuto ({type(e)}): {str(e)}"}
    
    return {"explanation": result["explanation"]}


# Caricamento dataset (.jsonl o .csv) su GCS
@app.post("/upload-alerts")
async def upload_alerts(file: UploadFile = File(...)):
    # Controllo estensione file ricevuto
    if not file.filename.endswith(".jsonl"):
        res.logger.warning(f"[VMS][app][upload_alerts] Invalid file extension: {file.filename}")
        raise HTTPException(status_code=400, detail="Formato file non supportato. Estensioni valide: jsonl")
    
    try:
        gcs.upload_to(res.gcs_dataset_dir, file.filename)
        res.logger.info(f"[VMS][app][upload_alerts] File {file.filename} uploaded")
        
        return {"filename": file.filename, "message": "File caricato con successo"}

    except Exception as e:
        res.logger.error(f"[VMS][app][upload_alerts] Failed to upload ({type(e)}): {str(e)}")
        raise HTTPException(status_code=500, detail=f"Errore nel caricamento del file: {str(e)}")


# Analisi dataset remoto (già caricato su GCS)
@app.post("/analyze-alerts")
async def analyze_alerts(dataset_filename: str = Query(...)):
    try:        
        response = await call_runner(
            method="GET",
            url=f"{res.runner_url}/run-dataset?dataset_filename={dataset_filename}"
        )

        res.logger.info("[VMS][app][analyze_alerts] Request sent to the runner")

    except Exception as e:
        res.logger.error(f"[VMS][app][analyze_alerts] Failed to send request ({type(e)}): {str(e)}")
        return {"message": f"Errore sconosciuto ({type(e)}): {str(e)}"}
    
    return response

# Flusso di operazioni:
# 1) Check   = [U] invio GET a /check-status -> [VMS] invio GET a / (root) = restituzione stato di VMS e CRR
# 2) Chat    = [U] invio alert a /chat -> [VMS] invio alert a /run-alert -> [CRR] analisi singolo alert = restituzione alert classificato
# 3) Dataset = [U] invio file dataset a /upload-alerts = restituzione esito operazione di caricamento file
#              [U] invio nome dataset a /analyze-alerts -> [VMS] invio nome dataset a /run-dataset -> [CRR] analisi dataset = restituzione esito operazione d'avviamento analisi
#              NB: il CRR si occupa automaticamente di suddividere il dataset in batch, di analizzare questi uno ad uno, alert dopo alert;
#                  gli alert classificati sono poi raggruppati per batch e salvati in file "result-batchID.json" temporanei, in attesa che la CRF li unisca nel file finale "result.json".