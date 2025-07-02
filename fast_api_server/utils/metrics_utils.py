import datetime


# Calcolo durata totale di analisi del dataset
def compute_duration(metadata: dict, metrics: list[dict]) -> float:
    # Estrazione numero batch totale
    n_batch = metadata.get("num_batch")
    if n_batch is None:
        raise ValueError("Undefined 'n_batch' field in metadata")
    
    # Ricerca timestamp (unico ciclo, doppia direzione di ricerca)
    t1_entry = t2_entry = None
    i, j = 0, len(metrics) - 1

    while i <= j and (t1_entry is None or t2_entry is None):    # True finché non trovo entrambi i timestamp o i due indici si incrociano
        if t1_entry is None:    # ricerca crescente (da 0 a N-1) -> si assume che il batch 0 sia a inizio lista
            m1 = metrics[i]
            if m1.get("batch_id") == 0:
                t1_entry = m1
            i += 1
        if t2_entry is None:    # ricerca decrescente (da N-1 a 0) -> si assume che il batch <n_batch-1> sia a fine lista
            m2 = metrics[j]
            if m2.get("batch_id") == n_batch - 1:
                t2_entry = m2
            j -= 1

    if not t1_entry or "timestamp" not in t1_entry:
        raise ValueError("Undefined 'timestamp' field in batch 0 metrics")
    if not t2_entry or "timestamp" not in t2_entry:
        raise ValueError(f"Undefined 'timestamp' field in batch {n_batch - 1} metrics")

    t1 = datetime.fromisoformat(t1_entry["timestamp"])
    t2 = datetime.fromisoformat(t2_entry["timestamp"])
    return (t2 - t1).total_seconds()


# Calcolo di durata e consumo RAM medi durante analisi batch
def compute_avg_time_and_ram(metrics: list[dict]) -> tuple[float | None, float | None]:
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
    return avg_time, avg_ram
