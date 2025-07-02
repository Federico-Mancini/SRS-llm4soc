import os, io, time, psutil, csv
from utils.resource_manager import resource_manager as res


# RAM usata dal processo
def get_memory_usage_mb() -> float:
    process = psutil.Process(os.getpid())
    return process.memory_info().rss / 1024 / 1024


# Inizio misurazione RAM/tempistiche
def init_monitoring():
    return time.perf_counter()

# Fine misurazione RAM/tempistiche
def finalize_monitoring(start_time, batch_id, n_alerts) -> dict:
    elapsed = time.perf_counter() - start_time
    ram = get_memory_usage_mb()

    return {
        "batch_id": batch_id,
        "n_alerts": n_alerts,
        "time_sec": round(elapsed, 2),  # tempo impiegato per analizzare il batch (secondi)
        "ram_mb": round(ram, 2)         # spazio d'archiviazione usato in RAM durante l'analisi (MB)
    }

# Upload metriche su GCS
def upload_metrics_to_gcs(metrics: list[dict], path: str):
    output = io.StringIO()
    writer = csv.DictWriter(output, fieldnames=["batch_id", "n_alerts", "time_sec", "ram_mb"])
    writer.writeheader()
    writer.writerows(metrics)

    res.bucket.blob(path).upload_from_string(output.getvalue(), content_type="text/csv")

    output.close()
