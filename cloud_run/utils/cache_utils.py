import json, hashlib
import utils.gcs_utils as gcs

from google.cloud import storage


bucket = None
GCS_CACHE_DIR = ""


# -- Funzioni -------------------------------------------------

# Inizializzazione var. d'ambiente (l'IF previene inizializzazioni ripetute)
def initialize():
    global bucket, GCS_CACHE_DIR

    if bucket is None:
        # Estrazione di variabili d'ambiente (condivise su GCS)
        conf = gcs.download_config()
        ASSET_BUCKET_NAME = conf["asset_bucket_name"]
        GCS_CACHE_DIR = conf["gcs_cache_dir"]

        # Connessione al bucket
        bucket = storage.Client().bucket(ASSET_BUCKET_NAME)



# Compute hash from alert
def alert_hash(alert: dict) -> str:
    raw = json.dumps(alert, sort_keys=True)
    return hashlib.md5(raw.encode()).hexdigest()


# Salva cache (un file di cache per ogni alert)
def save_alert_cache(data: dict):
    initialize()

    h = alert_hash(data)    # calcolo dell'impronta dell'alert
    blob = bucket.blob(f"{GCS_CACHE_DIR}/{h}.json")

    blob.upload_from_string(json.dumps(data))

    #TODO: inserire qui rimozione di file di cache che hanno superato periodo di validità (MAX_CACHE_AGE)


# Leggi cache
def load_alert_cache(h: str) -> dict | None:
    initialize()
    
    blob = bucket.blob(f"{GCS_CACHE_DIR}/{h}.json")
    
    if blob.exists():
        return json.loads(blob.download_as_text())
    
    return None
