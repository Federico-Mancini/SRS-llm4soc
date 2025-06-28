import json, time, hashlib
from utils.resource_manager import ResourceManager


res = ResourceManager()


# Compute hash from alert
def alert_hash(alert: dict) -> str:
    raw = json.dumps(alert, sort_keys=True)
    return hashlib.md5(raw.encode()).hexdigest()


# Salva cache (un file di cache per ogni alert)
def upload_alert_cache(data: dict):
    res.initialize()

    h = alert_hash(data)    # calcolo dell'impronta dell'alert
    blob = res.bucket.blob(f"{res.gcs_cache_dir}/{h}.json")

    blob.upload_from_string(json.dumps(data))

    cleanup()   # rimozione cache meno recente


# Leggi cache
def download_alert_cache(h: str) -> dict | None:
    res.initialize()
    
    blob = res.bucket.blob(f"{res.gcs_cache_dir}/{h}.json")
    
    if blob.exists():
        return json.loads(blob.download_as_text())
    
    return None


# Pulizia cache meno recente
def cleanup():
    res.initialize()

    prefix = f"{res.gcs_cache_dir}/"
    blobs = res.bucket.list_blobs(prefix=prefix)

    now = time.time()
    deleted = 0

    for blob in blobs:
        try:
            data = json.loads(blob.download_as_text())
            last_mod = data.get("last_modified", 0)

            if now - last_mod > res.max_cache_age:
                blob.delete()
                deleted += 1
        except Exception as e:
            print(f"[cache_utils|cleanup] Errore durante controllo cache {blob.name}: {e}")

    print(f"Pulizia cache completata - File rimossi: {deleted}")
