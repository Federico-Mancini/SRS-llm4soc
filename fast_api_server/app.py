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
        gcs.empty_dir(res.gcs_batch_metrics_dir)    # svuotamento directory dei batch metrics file
        gcs.empty_dir(res.gcs_batch_result_dir)     # svuotamento directory dei batch result file
        release_merge_lock()                        # uscita del merge handler dalla sospensione delle sue attività (eliminazione flag)

        # Lettura metadati del dataset
        metadata = download_metadata(dataset_filename)
        batch_size = res.batch_size                 # letto qui per avere il valore più recente/aggiornato (invece che in 'create_metadata')
        n_batches = max(1, (metadata["num_rows"] + batch_size - 1) // batch_size)

        metadata["num_batches"] = n_batches         # assegnazione dei dati mancanti
        metadata["batch_size"] = batch_size

        upload_metadata (dataset_filename, metadata) # upload dei nuovi metadati su GCS
        
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


#
@app.get("/errors")
def find_errors(dataset_filename: str = Query(...)):
    blob_path = gcs.get_blob_path(res.gcs_result_dir, dataset_filename, "result", "json")
    
    try:
        data = gcs.read_json(blob_path)
        error_ids = [
            item['batch_id']
            for item in data
            if isinstance(item, dict) and item.get('class') == 'error'
        ]
        return { "error_batch_ids": error_ids }
    
    except Exception as e:
        msg = f"[app|E__]\t\t-> Failed to read '{blob_path}' ({type(e).__name__}): {str(e)}"
        res.logger.error(msg)
        raise HTTPException(status_code=500, detail=msg)



# -- ANALISI METRICHE -----------------------------------------------------------------------------

# E08 - Visualizzazione metriche dell'ultima analisi effettuata sul dataset specificato (metriche nel JSON, non CSV)
@app.get("/metrics")
def get_metrics(dataset_filename: str = Query(...)):
    blob_path = gcs.get_blob_path(res.gcs_metrics_dir, dataset_filename, "metrics", "json")
    local_path = res.vms_metrics_path

    try:
        # Lettura dati
        gcs.download_to(blob_path, local_path)
        return iou.read_json(local_path)
    except Exception as e:
        msg = f"[app|E08]\t\t-> Failed to read '{local_path}' ({type(e).__name__}): {str(e)}"
        res.logger.error(msg)
        raise HTTPException(status_code=500, detail=msg)


# E09 - Analisi metriche dell'ultima analisi effettuata sul dataset specificato (metriche nel JSON, non CSV)
@app.get("/analyze-metrics")
def analyze_metrics(dataset_filename: str = Query(...)):
    metadata_blob_path = gcs.get_blob_path(res.gcs_dataset_dir, dataset_filename, "metadata", "json")
    metrics_blob_path = gcs.get_blob_path(res.gcs_metrics_dir, dataset_filename, "metrics", "json")
    metrics = None

    try:
        metadata = gcs.read_json(metadata_blob_path)
        metrics = gcs.read_json(metrics_blob_path)
    except Exception as e:
        msg = f"[app|E09]\t\t-> Failed to read '{metrics_blob_path}' ({type(e).__name__}): {str(e)}"
        res.logger.error(msg)
        raise HTTPException(status_code=500, detail=msg)

    # Calcolo metriche
    tot_time = mtr.compute_duration(metrics)
    avg_time, avg_ram = mtr.compute_avg_time_and_ram(metrics)
    min_time, min_ram, min_t_id, min_r_id = mtr.get_min_time_and_ram(metrics)
    max_time, max_ram, max_t_id, max_r_id = mtr.get_max_time_and_ram(metrics)
    a_throughput, b_throughput = mtr.compute_throughput(metadata, tot_time)
    std_time, std_ram = mtr.compute_standard_deviation(metrics)
    cv_time, cv_ram = mtr.compute_cv(std_time, std_ram, avg_time, avg_ram)

    # Costruzione entry CSV
    csv_row = {
        # Parametri di input / configurazione
        "max_concurrent_reqs": res.max_concurrent_requests,
        "batch_size": metadata.get("batch_size", 0),    # alert in ogni batch
        "num_batches": metadata.get("num_batches", 0),
        "num_rows": metadata.get("num_rows", 0),        # alert totali
        "num_cols": metadata.get("num_columns", 0),     # feature presenti in ogni alert
        
        # Metriche di performance principali
        "tot_time": tot_time,
        "avg_time": avg_time,
        "avg_ram": avg_ram,

        # Variabilità
        "std_time": std_time,
        "std_ram": std_ram,

        # Efficienza
        "alert_throughput": a_throughput if a_throughput is not None else 0,
        "batch_throughput": b_throughput if b_throughput is not None else 0,
    }

    try:
        # Upload entry in append al dataset usato per l'addestramento del Linear Regressor (per fare tuning dei parametri in config.json)
        if gcs.append_to_training_dataset(csv_row):
            gcs.download_to(res.ml_dataset_filename, res.vms_ml_dataset_path)
        #training_data = gcs.read_training_dataset()
    except Exception as e:
        msg = f"[app|E09]\t\t-> {type(e).__name__}: {str(e)}"
        res.logger.error(msg)
        raise HTTPException(status_code=500, detail=msg)
        
    # Alias locale
    f = mtr.format_metrics

    return {
        # ID dataset
        "dataset": dataset_filename,

        # Parametri di input / configurazione
        "alerts": metadata.get("num_rows", res.not_available),
        "batches": metadata.get("num_batches", res.not_available),
        "batch_size": metadata.get("batch_size", res.not_available),
        "max_concurrent_reqs": res.max_concurrent_requests,

        # Metriche temporali
        "tot_time": f(tot_time, "sec"),                        # durata totale
        "avg_time": f(avg_time, "sec"),                        # durata media
        "std_time": f(std_time, "sec"),                        # deviazione standard durate
        "cv_time": f(cv_time, "%", 1),                         # coefficiente varianza durate
        "min_time": f(min_time, f"sec (batch {min_t_id})"),    # durata minima registrata
        "max_time": f(max_time, f"sec (batch {max_t_id})"),    # durata massima registrata
        
        # Metriche spaziali
        "avg_ram": f(avg_ram, "MB"),                           # consumo RAM medio
        "std_ram": f(std_ram, "MB"),                           # deviazione standard consumi RAM
        "cv_ram": f(cv_ram, "%", 1),                           # coefficiente varianza consumi RAM
        "min_ram": f(min_ram, f"MB (batch {min_r_id})"),       # consumo RAM minimo registrato
        "max_ram": f(max_ram, f"MB (batch {max_r_id})"),       # consumo RAM massimo registrato

        # Metriche di efficienza
        "alert_throughput": f(a_throughput, "alert/sec"),      # alert elaborati al secondo
        "batch_throughput": f(b_throughput, "batch/sec"),      # batch elaborati al secondo    
    }

    # Deviazione Standard (std):
    # Misura la lontananza dei singoli valori dalla media.
    # Utile per valutare stabilità e prevedibilità di durate e consumi: con std bassa, la stabilità aumenta
    # Ha la stessa unità di misura dei dati analizzati (sec per le durate, MB per i consumi di RAM).

    # Coefficiente di variazione (cv):
    # Misura la variabilità relativa rispetto alla media: più la percentuale è bassa, più i dati sono coerenti (vicini alla media).
    # Percentuali maggiori del 15% sono indicative di dati poco coerenti.
    # Non ha unità di misura.
