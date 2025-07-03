import statistics
from resource_manager import resource_manager as res


# Formattazione dati finali visualizzati nella risposta HTTP
def format_metrics(value, suffix: str = "", precision: int = 2, fallback=res.not_available) -> str:
    try:
        if value is None or value == fallback:
            return fallback
        return f"{value:.{precision}f} {suffix}".strip()
    except (TypeError, ValueError):
        return fallback


# Calcolo durata totale di analisi dataset
def compute_duration(metrics: list[dict]):
    earliest_start = None   # timestamp di inizio analisi del primo batch
    latest_end = None       # timestamp di fine analisi dell'ultimo batch

    for m in metrics:
        ts = m.get("timestamp")         # timestamp istante inizio analisi
        duration = m.get("time_sec")    # durata analisi

        if not isinstance(ts, (int, float)):
            continue

        # Trova il timestamp iniziale minimo
        if earliest_start is None or ts < earliest_start:
            earliest_start = ts

        # Calcola la fine effettiva del batch
        if isinstance(duration, (int, float)):
            batch_end_time = ts + duration
            if latest_end is None or batch_end_time > latest_end:
                latest_end = batch_end_time

    if earliest_start is None or latest_end is None:
        return None

    return latest_end - earliest_start


# Calcolo durata e consumo RAM medi di analisi batch
def compute_avg_time_and_ram(metrics: list[dict]):
    total_time = total_ram = 0.0    # somme cumulative di durate e consumi RAM
    count_time = count_ram = 0      # numero di batch esaminati

    for m in metrics:
        t = m.get("time_sec")
        if isinstance(t, (int, float)):
            total_time += t
            count_time += 1
        r = m.get("ram_mb")
        if isinstance(r, (int, float)):
            total_ram += r
            count_ram += 1

    avg_time = total_time / count_time if count_time else None
    avg_ram = total_ram / count_ram if count_ram else None

    return avg_time, avg_ram


# Ricerca durata e consumo RAM minimi
def get_min_time_and_ram(metrics: list[dict]):
    min_time = min_time_batch_id = None
    min_ram = min_ram_batch_id = None

    for m in metrics:
        t = m.get("time_sec")
        if isinstance(t, (int, float)):
            if min_time is None or t < min_time:
                min_time = t
                min_time_batch_id = m.get("batch_id")

        r = m.get("ram_mb")
        if isinstance(r, (int, float)):
            if min_ram is None or r < min_ram:
                min_ram = r
                min_ram_batch_id = m.get("batch_id")

    return min_time, min_ram, min_time_batch_id, min_ram_batch_id


# Ricerca durata e consumo RAM massimi
def get_max_time_and_ram(metrics: list[dict]):
    max_time = max_time_batch_id = None
    max_ram = max_ram_batch_id = None

    for m in metrics:
        t = m.get("time_sec")
        if isinstance(t, (int, float)):
            if max_time is None or t > max_time:
                max_time = t
                max_time_batch_id = m.get("batch_id")

        r = m.get("ram_mb")
        if isinstance(r, (int, float)):
            if max_ram is None or r > max_ram:
                max_ram = r
                max_ram_batch_id = m.get("batch_id")
    
    return max_time, max_ram, max_time_batch_id, max_ram_batch_id


# Calcolo throughout alert e batch
def compute_throughput(metadata: dict, tot_time: float | None):
    if tot_time is None:
        return None

    tot_alerts = metadata.get("num_rows")
    tot_batches = metadata.get("num_batches")

    alert_throughput = tot_alerts / tot_time if tot_alerts else None
    batch_throughput = tot_batches / tot_time if tot_batches else None

    return alert_throughput, batch_throughput


# Calcolo deviazione standard di alert e batch
def compute_standard_deviation(metrics: list[dict]):
    if not metrics or len(metrics) < 2:
        return None, None  # con meno di due batch, non ha senso calcolare la deviazione standard

    try:
        std_time = statistics.stdev(m["time_sec"] for m in metrics)
        std_ram = statistics.stdev(m["ram_mb"] for m in metrics)
        return std_time, std_ram
    except (KeyError, statistics.StatisticsError):
        return None, None


# Calcolo di coefficiente di varianza di alert e batch
def compute_cv(
    std_time: float | None,
    std_ram: float | None,
    avg_time: float | None,
    avg_ram: float | None
):
    cv_time = std_time / avg_time * 100 if std_time and avg_time else None
    cv_ram = std_ram / avg_ram * 100 if std_ram and avg_ram else None
    
    cv_time_str = f"{cv_time:.1f}%" if cv_time is not None else "n/a"
    cv_ram_str = f"{cv_ram:.1f}%" if cv_ram is not None else "n/a"

    return cv_time, cv_ram
