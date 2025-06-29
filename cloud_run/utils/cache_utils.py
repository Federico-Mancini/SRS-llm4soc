import json, time, hashlib
from utils.resource_manager import ResourceManager


res = ResourceManager()


# Compute hash from alert
def alert_hash(alert: dict) -> str:
    raw = json.dumps(alert, sort_keys=True)
    return hashlib.md5(raw.encode()).hexdigest()


# Pulizia cache meno recente
def cleanup_cache():
    blobs = res.bucket.list_blobs(prefix=f"{res.gcs_cache_dir}/")
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
            res.logger.warning(f"[CRR][cache_utils][cleanup] Failed cleanup of {blob.name}: {e}")

    res.logger.debug(f"[CRR][cache_utils][cleanup] Cleanup completed. Deleted files: {deleted}")


# Salva cache (un file di cache per ogni alert)
def upload_cache(data: dict):
    h = alert_hash(data)    # calcolo dell'impronta dell'alert
    blob = res.bucket.blob(f"{res.gcs_cache_dir}/{h}.json")

    blob.upload_from_string(json.dumps(data))


# Leggi cache
def download_cache(h: str) -> dict | None:    
    blob = res.bucket.blob(f"{res.gcs_cache_dir}/{h}.json")
    
    if blob.exists():
        return json.loads(blob.download_as_text())
    
    return None
