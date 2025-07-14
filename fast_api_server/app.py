# VMS: Virtual Machine Server

import os ,json, posixpath
import utils.gcs_utils as gcs
import utils.io_utils as iou
import utils.metrics_utils as mtr

from fastapi import FastAPI, HTTPException, UploadFile, File, Request, Query
from fastapi.middleware.cors import CORSMiddleware
from utils.resource_manager import resource_manager as res
from utils.cloud_utils import call_worker, enqueue_batch_analysis_tasks
from utils.lock_utils import release_merge_lock
from utils.metadata_utils import create_metadata, download_metadata, upload_metadata



# == API configuration ============================================================================

app = FastAPI()
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],    # TODO: prima di pushare in produzione, da sostituire con URL di API in frontend (compito Samu)
    allow_credentials=True,
    allow_methods=["GET", "POST", "OPTIONS"],
    allow_headers=["Content-Type", "Authorization"],
)
# Comando per lanciare il server:
#   uvicorn app:app --host 0.0.0.0 --port 8000


# F01 - Function 1
@app.on_event("startup")
async def startup_event():
    res.logger.info("[app|F01]\t\t-> Virtual Machine Server - Status: running")

    # Controllo esistenza file di configurazione locale
    path = res.vms_config_path
    if not os.path.isfile(path):
        msg = f"[app|F01]\t\t-> Config file '{path}' not found. Aborting startup"
        res.logger.error(msg)
        raise RuntimeError(msg)
    
    # Upload file su GCS
    blob = res.bucket.blob(res.config_filename)
    blob.upload_from_filename(path)

    res.logger.info(f"[app|F01]\t\t-> File '{path}' uploaded to GCS as '/{res.config_filename}'")



# == Endpoints ====================================================================================
# -- ANALISI SERVER -------------------------------------------------------------------------------

# E01 - Endpoint 1
@app.api_route("/", methods=["GET", "POST"])
async def block_root():
    raise HTTPException(status_code=404, detail="Invalid endpoint")


# E02 - Check di stato di server (VM) e worker (Cloud Run)
@app.get("/health")
async def health_check():
    try:
        data = await call_worker("GET", f"{res.worker_url}/check-status")
        msg = data.get("status", "unknown")
    except Exception as e:
        msg = f"[app|E02]\t\t-> {type(e).__name__}: {str(e)}"
        res.logger.error(msg)
        raise HTTPException(status_code=500, detail=msg)

    return {"server (VMS)": "running", "worker (CRW)": msg}


# E03 - Check numero di file result temporanei creati fino al momento della chiamata
@app.get("/batch-results-status")
async def check_batch_results():
    try:
        blobs = res.bucket.list_blobs(prefix=res.gcs_batch_result_dir + "/")
        count = 0
        dataset_name = None
        metadata = {}
        
        for blob in blobs:
            filename = os.path.basename(blob.name)

            if "_result_" in filename and filename.endswith(".jsonl"):
                count += 1
            
                if not dataset_name:
                    dataset_name = os.path.basename(blob.name).split("_result_")[0]     # es: "ABC_result_0.jsonl" -> "ABC"
                    metadata = download_metadata(dataset_name) or {}
        
        if not dataset_name or not metadata:
            raise ValueError(f"Undefined 'dataset_name' field. This field will be available once the first batch result file has been loaded. Try again in a few seconds")
        if not metadata:
            raise FileNotFoundError(f"Metadata of '{dataset_name}' not found")

        batches = metadata.get("num_batches")
        if not isinstance(batches, int) or batches < 1:
            raise ValueError(
                "The 'num_batches' field is undefined: the dataset may not have been analyzed yet, "
                "still processing, or missing metadata. Try again later or re-upload the dataset to regenerate metadata"
            )

        status = "pending" if count == 0 else "partial" if count < batches else "completed"
        completion_rate = f"{count}/{batches} batches analyzed" if batches > 0 else res.not_available

        return {
            "status": status,
            "completion_rate": completion_rate,
            "dataset_name": dataset_name or res.not_available
        }

    except Exception as e:
        msg = f"[app|E03]\t\t-> {type(e).__name__}: {str(e)}"
        res.logger.error(msg)
        raise HTTPException(status_code=500, detail=msg)



# -- ANALISI ALERT --------------------------------------------------------------------------------

# E04 - Analisi singolo alert
@app.post("/chat")
async def chat(request: Request):
    try:
        data = await request.json() # question, data
        return await call_worker(
            method="POST",
            url=f"{res.worker_url}/run-chatbot",
            json=data
        )

    except Exception as e:
        msg = f"[app|E04]\t\t-> {type(e).__name__}: {str(e)}"
        res.logger.error(msg)
        raise HTTPException(status_code=500, detail=msg)


# E05 - Upload dataset (.jsonl o .csv) e relativi metadati su GCS
@app.post("/upload-dataset")
async def upload_dataset(file: UploadFile = File(...)):
    # NB: se un file con lo stesso nome è già presente su GCS, viene sovrascritto
    dataset_filename = file.filename

    # Controllo estensione file ricevuto
    if not dataset_filename.endswith((".json", ".jsonl", ".csv")):
        msg = f"[app|E05]\t\t-> Invalid file format: '{dataset_filename}' is not '.json', '.jsonl' or '.csv'"
        res.logger.warning(msg)
        raise HTTPException(status_code=400, detail=msg)
    
    try:
        # Upload dataset
        data = await file.read()
        dataset_path = posixpath.join(res.gcs_dataset_dir, dataset_filename)
        res.bucket.blob(dataset_path).upload_from_string(data, content_type=file.content_type)

        res.logger.info(f"[app|E05]\t\t-> Dataset file '{dataset_filename}' uploaded to '{dataset_path}'")

        # Upload metadata dataset
        metadata = create_metadata(dataset_filename)
        metadata_path = gcs.get_blob_path(res.gcs_dataset_dir, dataset_filename, "metadata", "json")
        
        blob = res.bucket.blob(metadata_path)
        blob.upload_from_string(
            json.dumps(metadata, indent=2),
            content_type="application/json"
        )

        res.logger.info(f"[app|E05]\t\t-> Metadata uploaded to '{metadata_path}'")

        return {
            "status": "completed",
            "dataset_path": dataset_path,
            "metadata_path": metadata_path,
            "metadata": metadata
        }
    
    except Exception as e:
        msg = f"[app|E05]\t\t-> Failed to upload '{dataset_filename}' ({type(e).__name__}): {str(e)}"
        res.logger.error(msg)
        raise HTTPException(status_code=500, detail=msg)


# E06 - Analisi dataset remoto (già caricato su GCS tramite '/upload-alerts')
@app.get("/analyze-dataset")
async def analyze_dataset(dataset_filename: str = Query(...)):
    try:
        # Pulizia e preparazione
        await gcs.empty_dir(res.gcs_batch_metrics_dir)  # svuotamento directory dei batch metrics file
        await gcs.empty_dir(res.gcs_batch_result_dir)   # svuotamento directory dei batch result file
        release_merge_lock()                            # uscita del merge handler dalla sospensione delle sue attività (eliminazione flag)
        res.logger.info("[app|E06]\t\t-> Directory emptied and lock released")

        # Lettura metadati del dataset
        metadata = download_metadata(dataset_filename)
        batch_size = res.batch_size                 # letto qui per avere il valore più recente/aggiornato (invece che in 'create_metadata')
        n_batches = max(1, (metadata["num_rows"] + batch_size - 1) // batch_size)

        metadata["num_batches"] = n_batches         # assegnazione dei dati mancanti
        metadata["batch_size"] = batch_size

        upload_metadata(dataset_filename, metadata) # upload dei nuovi metadati su GCS
        res.logger.info("[app|E06]\t\t-> Metadata updated and uploaded on GCS")

        # Creazione e analisi dei singoli batch tramite Cloud Task
        enqueue_batch_analysis_tasks(metadata)

        return {
            "status": "analysis started",
            "message": "Metadata extracted successfully. Batch slicing has been started in the background",
            "metadata": metadata
        }
    
    except Exception as e:
        msg = f"[app|E06]\t\t-> {type(e).__name__}: {str(e)}"
        res.logger.error(msg)
        raise HTTPException(status_code=500, detail=msg)


# E07 - Visualizzazione file con alert classificati
@app.get("/result")
def get_result(dataset_filename: str = Query(...)):
    blob_path = gcs.get_blob_path(res.gcs_result_dir, dataset_filename, "result", "json")
    local_path = res.vms_result_path

    try:
        # Lettura dati
        gcs.download_to(blob_path, local_path)
        return iou.read_json(local_path)
    except Exception as e:
        msg = f"[app|E07]\t\t-> Failed to read '{local_path}' ({type(e).__name__}): {str(e)}"
        res.logger.error(msg)
        raise HTTPException(status_code=500, detail=msg)



# -- ALTRO ----------------------------------------------------------------------------------------

# E10 - Aggiornamento variabili d'ambiente modificate a runtime (in particolare, dal benchmark)
@app.get("/reload-config")
async def reload_config():
    res.reload_config()
    return {"message": "Resource manager reloaded"}
