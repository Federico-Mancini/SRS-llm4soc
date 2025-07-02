import os, time, psutil


# RAM usata dal processo
def get_memory_usage_mb() -> float:
    process = psutil.Process(os.getpid())
    return process.memory_info().rss / 1024 / 1024


# Inizio misurazione consumi
def init_monitoring():
    return time.perf_counter()


# Fine misurazione consumi
def finalize_monitoring(start_time, batch_id, n_alerts) -> dict:
    elapsed = time.perf_counter() - start_time
    ram = get_memory_usage_mb()

    return {
        "batch_id": batch_id,
        "n_alerts": n_alerts,
        "ram_mb": round(ram, 2),        # spazio d'archiviazione usato in RAM durante l'analisi (MB)
        "time_sec": round(elapsed, 2),  # tempo impiegato per analizzare il batch (secondi)
        "timestamp": time.time()
    }
