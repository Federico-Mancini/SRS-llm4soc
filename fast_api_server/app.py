# VMS: Virtual Machine Server

import os ,json, posixpath
import utils.gcs_utils as gcs

from fastapi import FastAPI, HTTPException, UploadFile, File, Request, Query
from fastapi.middleware.cors import CORSMiddleware

from utils.resource_manager import resource_manager as res
from utils.task_utils import enqueue_tasks
from utils.auth_utils import call_runner


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
    res.logger.info("[VMS][app][startup_event] -> Virtual Machine Server - Status: running")

    path = res.vms_config_path
    if not os.path.isfile(path):
        msg = f"[VMS][app][startup_event] -> Config file '{path}' not found. Aborting startup"
        res.logger.error(msg)
        raise RuntimeError(msg)
    
    blob = res.bucket.blob(res.config_filename)
    blob.upload_from_filename(path)

    res.logger.info(f"[VMS][app][startup_event] -> File '{path}' uploaded to GCS as '/{res.config_filename}'")


# --- Endpoints ---------------------------------
@app.get("/")
def read_root():
    return {"status": "running"}


# Check di stato di API e runner
@app.get("/check-status")
async def check_status():
    try:
        data = await call_runner("GET", res.runner_url)
        msg = data.get("status", "unknown")

    except Exception as e:
        msg = f"[VMS][app][check_status] -> Error ({type(e).__name__}): {str(e)}"
        res.logger.error(msg)

    return {"server (VMS)": "running", "runner (CRR)": msg}
    # TODO: una volta eliminato il CRR, sostituirlo qui con chiamata al CRW


# Check numero di file result temporanei creati finora
@app.get("/monitor-batch-results")
async def monitor_batch_results():
    try:
        result_blobs = res.bucket.list_blobs(prefix=res.gcs_batch_result_dir + "/")
        batches = res.n_batches
        count = 0

        for blob in result_blobs:
            if blob.name.startswith("result_") and blob.name.endswith(".jsonl"):
                count += 1
                
        return {
            "status": "partial" if count > 0 else "pending",
            "batches_completed": count,
            "batches_to_be_done": batches-count if batches > 0 else "Warning! 'n_batches' is still in its default state (-1)"
        }

    except Exception as e:
        msg = f"[VMS][app][monitor_batch_results] -> Error ({type(e).__name__}): {str(e)}"
        res.logger.error(msg)
        raise HTTPException(status_code=500, detail=msg)


# Visualizzazione file con alert classificati
@app.get("/result")
def get_result(dataset_filename: str = Query(...)):
    dataset_name = os.path.splitext(dataset_filename)[0]
    gcs_result_path = posixpath.join(res.gcs_result_dir, f"{dataset_name}_result.json")
    
    # Controllo esistenza file remoto
    blob = res.bucket.blob(gcs_result_path)
    
    if not blob.exists():
        msg = f"[VMS][app][get_result] -> File '{gcs_result_path}' not found"
        res.logger.warning(msg)
        raise HTTPException(status_code=404, detail=msg)

    # Download file in locale
    blob.download_to_filename(res.vms_result_path)

    if not os.path.exists(res.vms_result_path):
        msg = f"[VMS][app][get_result] -> Downloaded file not found locally in '{res.vms_result_path}'"
        res.logger.warning(msg)
        raise HTTPException(status_code=404, detail=msg)
    
    # Lettura dati
    try:
        with open(res.vms_result_path, "r") as f:
            data = json.load(f)

        res.logger.info(f"[VMS][app][get_result] -> File '{res.vms_result_path}' read")
        return data
    
    except json.JSONDecodeError:
        msg = f"[VMS][app][get_result] -> Failed to parse '{res.vms_result_path}' ({type(e).__name__}): {str(e)}"
        res.logger.error(msg)
        raise HTTPException(status_code=500, detail=msg)
    
    except Exception as e:
        msg = f"[VMS][app][get_result] -> Failed to read '{res.vms_result_path}' ({type(e).__name__}): {str(e)}"
        res.logger.error(msg)
        raise HTTPException(status_code=500, detail=msg)


# Classificazione di singolo alert
@app.post("/chat")
async def chat(request: Request):
    try:
        alert_json = await request.json()
        
        result = await call_runner(
            method="POST",
            url=f"{res.runner_url}/run-alert",
            json={"alert": alert_json}
        )

    except Exception as e:
        msg = f"[VMS][app][chat] -> Failed to send request ({type(e).__name__}): {str(e)}"
        res.logger.error(msg)
        return {"explanation": msg}
    
    return {"explanation": result["explanation"]}


# Caricamento dataset (.jsonl o .csv) e relativi metadati su GCS
@app.post("/upload-dataset")    # NOTA! nome endpoint cambiato dall'ultima volta.
async def upload_alerts(file: UploadFile = File(...)):
    # NB: se un file con lo stesso nome è già presente su GCS, viene sovrascritto
    dataset_filename = file.filename

    # Controllo estensione file ricevuto
    if not dataset_filename.endswith((".jsonl", ".csv")):
        msg = f"[VMS][app][upload_alerts] -> Invalid file format: '{dataset_filename}' is not '.jsonl' or '.csv'"
        res.logger.warning(msg)
        raise HTTPException(status_code=400, detail=msg)
    
    try:
        # Upload dataset
        data = await file.read()
        dataset_path = posixpath.join(res.gcs_dataset_dir, dataset_filename)
        res.bucket.blob(dataset_path).upload_from_string(data, content_type=file.content_type)

        res.logger.info(f"[VMS][app][upload_alerts] -> Dataset file '{dataset_filename}' uploaded to '{dataset_path}'")

        # Upload metadata dataset
        metadata = gcs.get_dataset_metadata(dataset_filename)
        metadata_filename = os.path.splitext(dataset_filename)[0] + "_metadata.json"
        metadata_path = posixpath.join(res.gcs_dataset_dir, metadata_filename)
        res.bucket.blob(metadata_path).upload_from_string(json.dumps(metadata, indent=2), content_type="application/json")

        res.logger.info(f"[VMS][app][upload_alerts] -> Metadata file '{metadata_filename}' uploaded to '{metadata_path}'")

        return {
            "dataset": dataset_path,
            "metadata": metadata_path,
            "message": "File e metadati caricati con successo"
        }
    
    except Exception as e:
        msg = f"[VMS][app][upload_alerts] -> Failed to upload '{dataset_filename}' ({type(e).__name__}): {str(e)}"
        res.logger.error(msg)
        raise HTTPException(status_code=500, detail=msg)


# TODO: da rimuovere
# Analisi dataset remoto (già caricato su GCS)
# @app.get("/analyze-alerts")
# async def analyze_alerts(dataset_filename: str = Query(...)):
#     try:        
#         response = await call_runner(
#             method="GET",
#             url=f"{res.runner_url}/run-dataset?dataset_filename={dataset_filename}",
#             timeout=180.0   # 3 min (tempo massimo di suddivisione in batch di dataset medio-piccoli)
#         )

#         res.logger.info("[VMS][app][analyze_alerts] -> Request sent to the runner")

#     except Exception as e:
#         res.logger.error(f"[VMS][app][analyze_alerts] -> Failed to send request ({type(e)}): {str(e)}")
#         return {"message": f"Errore sconosciuto ({type(e)}): {str(e)}"}
    
#     return response


# Analisi dataset remoto (già caricato su GCS tramite '/upload-alerts')
@app.get("/analyze-dataset")
async def analyze_dataset(dataset_filename: str = Query(...)):
    try:
        # Pulizia e preparazione
        gcs.empty_dir(res.gcs_batch_result_dir) # svuotamento directory destinata ai risultati di analisi batch (fatto anche dalla CRF)

        # Estrazione metadati da dataset
        dataset_name = os.path.splitext(dataset_filename)[0]
        metadata_path = posixpath.join(res.gcs_dataset_dir, f"{dataset_name}_metadata.json")
        metadata_text = res.bucket.blob(metadata_path).download_as_text()
        metadata = json.loads(metadata_text)
        
        # Creazione e analisi dei singoli batch tramite Cloud Task
        enqueue_tasks(metadata)

        return {
            "status": "analysis started",
            "message": "Metadata extracted successfully. Batch slicing has been started in the background",
            "metadata": metadata
        }
    
    except Exception as e:
        msg = f"[VMS][app][analyze_dataset] -> Error ({type(e).__name__}): {str(e)}"
        res.logger.error(msg)
        raise HTTPException(status_code=500, detail=msg)