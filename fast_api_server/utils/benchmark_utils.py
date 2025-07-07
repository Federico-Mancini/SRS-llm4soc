# Benchmark Utils: modulo dedicato alla gestione automatizzata dell'analisi di un dataset, eseguita con parametri dinamici

import os, time, json, threading, shutil, requests
import utils.gcs_utils as gcs
import utils.io_utils as iou

from fastapi import HTTPException
from utils.resource_manager import resource_manager as res 
from utils.cloud_utils import call_worker


MAX_POLLING_ATTEMPTS = 20
DATASET_ANALYSIS = "analyze-dataset"
METRICS_ANALYSIS = "analyze-metrics"
RESULTS_CHECK = "batch-results-status"


# F01 - Analisi automatizzata di un dataset, con parametrizzazione variabile
async def run_benchmark(
    dataset_filename,
    batch_size_inf,
    batch_size_sup,
    batch_size_step,
    max_reqs_inf,
    max_reqs_sup,
    max_reqs_step
):
    # Eliminazione di eventuali flag residue
    if os.path.exists(res.vms_benchmark_stop_flag):
        os.remove(res.vms_benchmark_stop_flag)

    try:
        # Lettura metadati
        metadata_blob_path = gcs.get_blob_path(res.gcs_dataset_dir, dataset_filename, "metadata", "json")
        metadata = gcs.read_json(metadata_blob_path)
        tot_alerts = metadata["num_rows"]   # non uso '.get()' per evidenziare eventuali problemi tramite il lancio di un'eccezione
    except Exception as e:
        msg = f"[benchmark|F01]\t-> Failed to load metadata ({type(e).__name__}): {str(e)}"
        res.logger.error(msg)
        raise HTTPException(status_code=500, detail=msg)
    
    curr_batch_size = batch_size_inf
    curr_max_reqs = max_reqs_inf
    
    # Salvataggio backup del file 'config.json' e aggiornamento di contesto del benchmark
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
        curr_max_reqs = 4 if curr_batch_size > 100 else max_reqs_inf
        # NB: all'aumentare della dimensione dei batch, il numero di thread paralleli viene resettato a un nuovo limite inferiore pari a 4.
        #     Questo permette di velocizzare i tempi e di non considerare i casi estremi in cui viene elaborato un batch da centinaia
        #     di alert sequenzialmente.
        
        while curr_max_reqs <= max_reqs_sup:
            if check_stop_flag():
                res.logger.info("[benchmark|F01]\t-> Benchmark execution interrupted by stop flag")
                return

            # Aggiornamento variabili d'ambiente su JSON
            await update_config_json(curr_batch_size, curr_max_reqs)
            update_benchmark_context(
                tot_alerts=tot_alerts,
                batch_size=curr_batch_size,
                max_reqs=curr_max_reqs,
                last_request=f"/{DATASET_ANALYSIS}" # specificata qui per l'ampio transitorio che separa richiesta e risposta (potrebbe creare inconsistenza tra contesto e ciò che sta realmente accadendo)
            )

            # Invio richiesta HTTP ad '/analyze-dataset'
            def request_analysis():
                requests.get(f"http://localhost:8000/{DATASET_ANALYSIS}?dataset_filename={dataset_filename}", timeout=30)
            
            res.logger.info(f"[benchmark|F01]\t-> Sending request to '/{DATASET_ANALYSIS}'")
            threading.Thread(target=request_analysis).start()   # creazione thread separato per permettere al server di chiamare se stesso senza creare deadlock

            # Polling su '/monitor-batch-results'
            update_benchmark_context(last_request=f"/{RESULTS_CHECK}", status="polling")
            attempts = 0
            while True:
                if check_stop_flag():
                    res.logger.info("[benchmark|F01]\t-> Benchmark execution interrupted by stop flag")
                    return
                
                response = requests.get(f"http://localhost:8000/{RESULTS_CHECK}")
                
                # Gestione errori (sia i 404 previsti che quelli di natura ignota)
                if response.status_code != 200:             # NB: gli errori 404 previsti sono quelli che si verificano quando non è presente alcun file nella dir GCS '/batch_results' (risultati non ancora pronti)
                    if attempts >= MAX_POLLING_ATTEMPTS:    # non viene usato un 'while(true)' per prevenire eventuali loop infiniti in presenza di errori ignoti
                        update_benchmark_context(status="error")
                        restore_config()
                        res.logger.error(f"[benchmark|F01]\t-> Polling failed: '/{RESULTS_CHECK}' returned status {response.status_code}")
                        return
                    
                    res.logger.warning(f"[benchmark|F01]\t-> Attempt {attempts + 1}/{MAX_POLLING_ATTEMPTS}")
                    polling_period = 5 + attempts^2
                    time.sleep(polling_period if polling_period < 60 else 60)   # attesa esponenziale, con limite superiore fissato a 60
                    attempts += 1
                    continue
                
                if response.json().get("status") == "completed":
                    break
                
                res.logger.info("[benchmark|F01]\t-> Waiting to get all the batch result files")
                time.sleep(15 if tot_alerts < 500 else 30)

            # Invio richiesta HTTP ad '/analyze-metrics' (OBSOLETE)
            # res.logger.info(f"[benchmark|F01]\t-> Dataset analysis completed, now sending request to '/{METRICS_ANALYSIS}'")
            # requests.get(f"http://localhost:8000/{METRICS_ANALYSIS}?dataset_filename={dataset_filename}")
            # update_benchmark_context(last_request=f"/{METRICS_ANALYSIS}", status="running")
            res.logger.info(f"[benchmark|F01]\t-> Dataset analysis completed. Waiting 30 seconds as inter-analysis buffer")
            update_benchmark_context(status="running")

            curr_max_reqs = get_next_val(curr=curr_max_reqs, sup=max_reqs_sup, step=max_reqs_step)
            if curr_batch_size != batch_size_sup or curr_max_reqs != max_reqs_sup:  # se è l'ultima iterazione, non attendo 1 minuto
                time.sleep(30)  # apparentemente man mano che si inviano richieste senza sosta, i tempi d'elaborazione si allungano. Una breve pausa intermedia aiuta a tornare in margini accettabili

        curr_batch_size = get_next_val(curr=curr_batch_size, sup=batch_size_sup, step=batch_size_step)

    restore_config()
    update_benchmark_context(status="completed")
    res.logger.info("[benchmark|F01]\t-> Benchmark naturally completed")


# F02 - Salvataggio di variabili d'ambiente originali in un file copia temporaneo
def backup_config():
    if not os.path.exists(res.vms_config_backup_path):
        shutil.copy(res.vms_config_path, res.vms_config_backup_path)
        res.logger.info(f"[benchmark|F02]\t-> Backup of '{res.config_filename}' created")
    else:
        res.logger.warning("[benchmark|F02]\t-> Backup already exists")

# F03 - Ripristino delle variabili d'ambiente originali
def restore_config():
    if os.path.exists(res.vms_config_backup_path):
        shutil.copy(res.vms_config_backup_path, res.vms_config_path)
        os.remove(res.vms_config_backup_path)
        res.reload_config()
        res.logger.info(f"[benchmark|F03]\t-> Original '{res.config_filename}' restored and backup deleted")
    else:
        res.logger.warning("[benchmark|F03]\t-> No backup found to restore")


# F04 - Aggiornamento variabili d'ambiente in 'config.json'
async def update_config_json(batch_size, max_reqs):
    local_path = res.vms_config_path
    blob_path = res.config_filename

    # Lettura locale e aggiornamento dati
    with open(local_path, "r", encoding="utf-8") as f:
        config = json.load(f)

    config["batch_size"] = batch_size
    config["max_concurrent_requests"] = max_reqs

    # Aggiornamento 'config.json' locale e remoto
    iou.write_json(config, local_path)           # scrittura file locale
    gcs.upload_to(local_path, blob_path)    # scirttura file remoto

    try:
        # Aggiornamento dati salvati in resource manager locale (VMS) e remoto (CRW)
        res.reload_config()
        await call_worker("GET", f"{res.worker_url}/reload-config")
    except Exception as e:
        msg = f"[benchmark|F04]\t-> ({type(e).__name__}): {str(e)}"
        res.logger.error(msg)
        raise HTTPException(status_code=500, detail=msg)

    res.logger.info(f"[benchmark|F04]\t-> File '{res.config_filename}' updated (batch_size: {batch_size}, max_concurrent_reqs: {max_reqs})")

# F05 - Salvataggio contesto del benchmark
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

    if tot_alerts is not None:
        context["tot_alerts"] = tot_alerts
    else:
        tot_alerts = context["tot_alerts"]  # in un modo o nell'altro, serve che tot_alerts sia sempre definito per poterlo usare nei successivi calcoli
    
    if batch_size is not None or batch_size_sup is not None:
        current_val = context.get("batch_size", "0/0").split("/")
        if tot_alerts is None:
            tot_alerts = current_val[1]  # fallback: mantieni il limite superiore corrente
        new_val = batch_size if batch_size is not None else int(current_val[0])
        new_val_sup = batch_size_sup if batch_size_sup is not None else min(int(current_val[1]), int(tot_alerts))
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


# F06 - Controllo del flag di terminazione
def check_stop_flag() -> bool:
    if not os.path.exists(res.vms_benchmark_stop_flag):
        return False
    
    res.logger.warning("[benchmark|F06]\t-> Stop flag detected, benchmark aborted")
    os.remove(res.vms_benchmark_stop_flag)
    restore_config()
    update_benchmark_context(status="aborted")
    return True


# F07 - Incremento condizionato delle variabili di controllo
def get_next_val(curr: int, sup: int, step: int) -> int:
    # Troncatura del valore al limite superiore, in caso lo superi
    def cut(val: int) -> int:
        return val if val < sup else sup    # restituisce il valore o il limite superiore quando superato
    
    # Ricerca del primo sottomultiplo del limite superiore
    def get_first_submultiple() -> int | None:
        for i in range(curr+1, sup+1):  # il range ha inizio in 'curr+1': valori minori o uguali a 'curr' non interessano
            if i % step == 0:           # 'i != sup' non è presente in quanto questo caso è già intercettato nell'IF esterno: 'curr == sup'
                return i if i - curr > step / 2 else cut(i + step)  # se 'curr' è molto vicino a 'i', si preferisce assegnargli il multiplo successivo a quello prossimo 
        return None


    if curr == sup:                     # termine del ciclo che ha visto il 'curr_val' finale, uguale al limite superiore
        return curr + 1                 # valore flag usato per uscire dal ciclo while

    next_val = cut(curr + step)         # incremento con troncatura (in caso il nuovo valore superi il limite superiore)

    if next_val == sup or step == 1:    # limite raggiunto (restituzione immediata), incremento +1 (nessuna modifica richiesta)
        return next_val
    
    if sup % step == 0:                 # limite superiore multiplo del passo (se True, 'curr' viene allineato alla sequenza di sottomultipli)
        return get_first_submultiple() or next_val

    return next_val
