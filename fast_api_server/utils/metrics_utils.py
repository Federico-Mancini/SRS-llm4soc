# Calcolo durata totale di analisi del dataset
def compute_duration(metrics: list[dict]) -> str:
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
        return "n/a"

    total_duration = latest_end - earliest_start
    return f"{total_duration:.2f} sec"


# Calcolo di durata e consumo RAM medi durante analisi batch
def compute_avg_time_and_ram(metrics: list[dict]) -> tuple[str, str]:
    total_time = total_ram = 0.0    # somma cumulativa di valori
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

    avg_time_str = f"{avg_time:.2f} sec" if avg_time is not None else "n/a" # NB: impotrante specificare "not None", altrimenti "if <val>" potrebbe essere falso anche in presenza di 0.0
    avg_ram_str = f"{avg_ram:.2f} MB" if avg_ram is not None else "n/a"
    
    return avg_time_str, avg_ram_str


# Ricerca durata e consumo RAM minimi
def get_min_time_and_ram(metrics: list[dict]) -> tuple[str, str]:
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

    # NB: usare "is not None" per non escludere 0.0 come valore valido
    min_time_str = f"{min_time:.2f} sec (batch {min_time_batch_id})" if min_time is not None else "n/a"
    min_ram_str = f"{min_ram:.2f} MB (batch {min_ram_batch_id})" if min_ram is not None else "n/a"

    return min_time_str, min_ram_str


# Ricerca durata e consumo RAM massimi
def get_max_time_and_ram(metrics: list[dict]) -> tuple[str, str]:
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

    max_time_str = f"{max_time:.2f} sec (batch {max_time_batch_id})" if max_time is not None else "n/a" # NB: impotrante specificare "not None", altrimenti "if <val>" potrebbe essere falso anche in presenza di 0.0
    max_ram_str = f"{max_ram:.2f} MB (batch {max_ram_batch_id})" if max_ram is not None else "n/a"
    
    return max_time_str, max_ram_str
