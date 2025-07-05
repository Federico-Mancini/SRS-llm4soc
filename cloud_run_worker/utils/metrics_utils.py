import os, time, json, psutil
from utils.resource_manager import resource_manager as res


# RAM usata dal processo
def get_memory_usage_mb() -> float:
    process = psutil.Process(os.getpid())
    return process.memory_info().rss / 1024 / 1024


# Inizio misurazione consumi
def init_monitoring() -> tuple[float, float]:
    return time.perf_counter(), time.time()
    # time.perf_counter():  timer, usato per misurare durata analisi batch
    # time.time():          timestamp assoluto (epoch time), usato per registrare istante d'inizio di analisi batch


# Finalizzazione misurazioni
def finalize_monitoring(timer_start: float, timestamp_start: float, batch_id: int, batch_size: int) -> dict:
    elapsed = time.perf_counter() - timer_start
    ram = get_memory_usage_mb()

    concurrency = res.max_concurrent_requests
    parallelism_used = min(batch_size, concurrency)
    avg_time = elapsed / batch_size if batch_size else 0.0
    alert_throughput = batch_size / elapsed if elapsed else 0
    
    metrics = {
        "batch_id": batch_id,
        "batch_size": batch_size,               # numero alert contenuti in un batch
        "max_concurrent_reqs": concurrency,     # max numero di thread parallelizzabili con asyncio
        "parallelism_used": parallelism_used,   # numero di richieste parallele effettivamente inviate a Gemini
        "alert_throughput": alert_throughput,   # numero di alert processati al secondo
        "ram_mb": ram,                          # spazio d'archiviazione usato in RAM durante l'analisi (MB)
        "time_sec": elapsed,                    # tempo impiegato per analizzare il batch (secondi)
        "avg_time_per_alert": avg_time,         # tempo d'elaborazione medio di ogni alert
        "timestamp": timestamp_start            # timestamp istante inizio analisi del batch
    }

    return metrics


# Calcolo errori in batch e aggiornamento metriche
def update_metrics(batch_results: list[dict], batch_size: int, path: str) -> list[dict]:
    n_errors = sum(1 for r in batch_results if r.get("class") == "error")
    error_rate = n_errors / batch_size if batch_size else 0.0
    success_rate = 1 - n_errors / batch_size if batch_size else 0.0
    n_timeouts = sum("Timeout" in r.get("explanation", "") for r in batch_results)
    
    # Scarica metriche esistenti
    blob = res.bucket.blob(path)
    if not blob.exists():
        res.logger.error(f"[metrics|F__]\t-> File '{path}' not found")
        raise FileNotFoundError(f"File '{path}' not found")
    
    metrics_text = blob.download_as_text()
    metrics = json.loads(metrics_text)

    # Aggiungi le nuove metriche
    metrics["n_classified"] = batch_size - n_errors # classificazioni riuscite
    metrics["success_rate"] = success_rate          # tasso di classificazione riuscite
    metrics["has_errors"] = n_errors > 0            # flag utile per sapere immediatamente se ci sono errori nel batch
    metrics["n_errors"] = n_errors                  # classificazioni fallite
    metrics["error_rate"] = error_rate              # tasso di classificazione fallite
    metrics["n_timeouts"] = n_timeouts              # numero di errori dovuti a timeout

    return [metrics]

# Elenco nomi metriche (per header CSV):
# batch_id,batch_size,max_concurrent_reqs,parallelism_used,alert_throughput,ram_mb,time_sec,avg_time_per_alert,timestamp,n_classified,success_rate,has_errors,n_errors,error_rate,n_timeouts