import os, time, psutil


# RAM usata dal processo
def get_memory_usage_mb() -> float:
    process = psutil.Process(os.getpid())
    return process.memory_info().rss / 1024 / 1024


# Inizio misurazione consumi
def init_monitoring() -> tuple[float, float]:
    # time.perf_counter(): timer, usato per misurare durata analisi batch
    # time.time(): timestamp assoluto (epoch time), usato per registrare istante d'inizio di analisi batch
    return time.perf_counter(), time.time()


# Fine misurazione consumi
def finalize_monitoring(timer_start: float, timestamp_start: float, batch_id: int, n_alerts: int) -> dict:
    elapsed = time.perf_counter() - timer_start
    ram = get_memory_usage_mb()

    return {
        "batch_id": batch_id,
        "n_alerts": n_alerts,           # numero alert contenuti in un batch
        "ram_mb": round(ram, 2),        # spazio d'archiviazione usato in RAM durante l'analisi (MB)
        "time_sec": round(elapsed, 2),  # tempo impiegato per analizzare il batch (secondi)
        "timestamp": timestamp_start    # timestamp istante inizio analisi del batch
    }
