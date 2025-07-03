import os, time, json, shutil, requests
import utils.gcs_utils as gcs

from fastapi import HTTPException
from utils.resource_manager import resource_manager as res 
from utils.cloud_utils import call_worker
from utils.io_utils import upload_to_gcs


# Endpoint usati nel benchmark
DATASET_ANALYSIS = "analyze-dataset"
METRICS_ANALYSIS = "analyze-metrics"
RESULTS_CHECK = "batch-results-status"


# Analisi automatizzata di un dataset, con parametrizzazione variabile 
def run_benchmark(dataset_filename, batch_size_inf, batch_size_sup, batch_size_step, max_reqs_inf, max_reqs_sup, max_reqs_step):
    try:
        metadata_blob_path = gcs.get_blob_path(res.gcs_dataset_dir, dataset_filename, "metadata", "json")
        metadata = gcs.read_json(metadata_blob_path)
        tot_alerts = metadata["num_rows"]   # non uso '.get()' per evidenziare eventuali problemi tramite il lancio di un'eccezione
    except Exception as e:
        msg = f"[VMS][benchmark_utils][run_benchmark] -> Failed to load metadata: {str(e)}"
        res.logger.error(msg)
        raise HTTPException(status_code=500, detail=msg)
    
    curr_batch_size = batch_size_inf
    curr_max_reqs = max_reqs_inf
    
    backup_config()
    update_benchmark_context(
        dataset_filename=dataset_filename,
        tot_alerts=tot_alerts,
        batch_size=curr_batch_size,
        batch_size_sup=batch_size_sup,
        batch_size_step=batch_size_step,
        max_reqs=curr_max_reqs,
        max_reqs_sup=max_reqs_sup,
        max_reqs_step=max_reqs_step,
        status="running"
    )
    
    while curr_batch_size <= min(batch_size_sup, tot_alerts):
        curr_max_reqs = max_reqs_inf
        while curr_max_reqs <= max_reqs_sup:
            if check_stop_flag():
                res.logger.info("[VMS][benchmark_utils][run_benchmark] Benchmark aborted")
                return

            # Aggiornamento variabili d'ambiente su JSON
            update_config_json(curr_batch_size, curr_max_reqs)
            update_benchmark_context(
                batch_size=curr_batch_size,
                max_reqs=curr_max_reqs,
                last_request=f"/{DATASET_ANALYSIS}" # specificata qui per l'ampio transitorio che separa richiesta e risposta (potrebbe creare inconsistenza tra contesto e ciò che sta realmente accadendo)
            )

            # Invio richiesta HTTP ad '/analyze-dataset'
            res.logger.info(f"[VMS][benchmark_utils][run_benchmark] Now triggering '/{DATASET_ANALYSIS}'")
            requests.get(f"http://localhost:8000/{DATASET_ANALYSIS}?dataset_filename={dataset_filename}", timeout = 60)

            # Polling su '/monitor-batch-results'
            update_benchmark_context(last_request=f"/{RESULTS_CHECK}", status="polling")
            while True:
                time.sleep(15)
                response = requests.get(f"http://localhost:8000/{RESULTS_CHECK}")
                if response.status_code != 200:
                    restore_config()
                    msg = f"[VMS][benchmark_utils][run_benchmark] Polling failed: '/{RESULTS_CHECK}' returned status {response.status_code}"
                    res.logger.error(msg)
                    raise RuntimeError(msg)
                if response.json().get("status") == "completed":
                    break

            # Invio richiesta HTTP ad '/analyze-metrics'
            res.logger.info(f"[VMS][benchmark_utils][run_benchmark] Dataset analysis completed, now triggering '/{METRICS_ANALYSIS}'")
            response = requests.get(f"http://localhost:8000/{METRICS_ANALYSIS}?dataset_filename={dataset_filename}")
            update_benchmark_context(last_request=f"/{METRICS_ANALYSIS}", status="running")
            print(response.text)

            curr_max_reqs += max_reqs_step

        curr_batch_size += batch_size_step

    restore_config()
    update_benchmark_context(status="completed")
    res.logger.info("[VMS][benchmark_utils][run_benchmark] Benchmark completed")


# Salvataggio di variabili d'ambiente originali in un file copia temporaneo
def backup_config():
    if not os.path.exists(res.vms_config_backup_path):
        shutil.copy(res.vms_config_path, res.vms_config_backup_path)
        res.logger.info("[VMS][benchmark_utils][backup_config] Backup of 'config.json' created")
    else:
        res.logger.warning("[VMS][benchmark_utils][backup_config] Backup already exists")


# Ripristino delle variabili d'ambiente originali
def restore_config():
    if os.path.exists(res.vms_config_backup_path):
        shutil.copy(res.vms_config_backup_path, res.vms_config_path)
        os.remove(res.vms_config_backup_path)
        res.logger.info("[VMS][benchmark_utils][restore_config] Original 'config.json' restored and backup deleted")
    else:
        res.logger.warning("[VMS][benchmark_utils][restore_config] No backup found to restore")


# Aggiornamento variabili d'ambiente in 'config.json'
def update_config_json(batch_size, max_reqs):
    local_path = res.vms_config_path
    blob_path = res.config_filename

    # Lettura locale e aggiornamento dati
    with open(local_path, "r", encoding="utf-8") as f:
        config = json.load(f)

    config["batch_size"] = batch_size
    config["max_concurrent_requests"] = max_reqs

    # Scrittura locale e remota
    with open(local_path, "w", encoding="utf-8") as f:
        json.dump(config, f, indent=2)
    upload_to_gcs(local_path, blob_path)

    try:
        # Aggiornamento dati salvati in resource manager locale (VMS) e remoto (CRW)
        res.reload_config()
        call_worker("GET", f"{res.worker_url}/reload-config")
    except Exception as e:
        msg = f"[VMS][app][check_status] -> Error ({type(e).__name__}): {str(e)}"
        res.logger.error(msg)
        raise HTTPException(status_code=500, detail=msg)

    res.logger.info(f"[VMS][benchmark_utils][update_config_json] Updated 'config.json' (batch_size: {batch_size}, max_concurrent_reqs: {max_reqs})")


# Salvataggio contesto del benchmark
def update_benchmark_context(
    dataset_filename=None,
    tot_alerts=None,
    batch_size=None,
    batch_size_sup=None,
    batch_size_step=None,
    max_reqs=None,
    max_reqs_sup=None,
    max_reqs_step=None,
    last_request=None,
    status=None
):
    try:
        with open(res.vms_benchmark_context_path, "r", encoding="utf-8") as f:
            context = json.load(f)
    except Exception:
        context = {}

    if dataset_filename is not None:
        context["dataset_filename"] = dataset_filename

    if batch_size is not None or batch_size_sup is not None:
        current_val = context.get("batch_size", "0/0").split("/")
        new_val = batch_size if batch_size is not None else int(current_val[0])
        new_val_sup = batch_size_sup if batch_size_sup is not None else min(int(current_val[1]), tot_alerts)
        context["batch_size"] = f"{new_val}/{new_val_sup}"

    if batch_size_step is not None:
        context["batch_size_step"] = batch_size_step

    if max_reqs is not None or max_reqs_sup is not None:
        current_val = context.get("max_concurrent_requests", "0/0").split("/")
        new_val = max_reqs if max_reqs is not None else int(current_val[0])
        new_val_sup = max_reqs_sup if max_reqs_sup is not None else int(current_val[1])
        context["max_concurrent_requests"] = f"{new_val}/{new_val_sup}"

    if max_reqs_step is not None:
        context["max_reqs_step"] = max_reqs_step

    if last_request is not None:
        context["last_request"] = last_request  # ultima richiesta HTTP inviata

    if status is not None:
        context["status"] = status              # stato d'esecuzione: running, polling, completed

    context["last_updated"] = time.strftime("%Y-%m-%d %H:%M:%S")

    with open(res.vms_benchmark_context_path, "w", encoding="utf-8") as f:
        json.dump(context, f, indent=2)


# Check flag terminazione
def check_stop_flag() -> bool:
    if not os.path.exists(res.vms_benchmark_stop_flag):
        return False
    
    res.logger.warning("[VMS][benchmark_utils][check_stop_flag] Stop flag detected — benchmark aborted")
    os.remove(res.vms_benchmark_stop_flag)
    restore_config()
    update_benchmark_context(status="aborted")
    return True

