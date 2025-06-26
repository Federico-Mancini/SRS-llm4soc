import os, json

from google.cloud import storage
from utils.config import ASSET_DIR, RESULT_FILENAME, ASSET_BUCKET_NAME,\
    GCS_CACHE_DIR, GCS_RESULT_DIR, MAX_CONCURRENT_REQUESTS, MAX_CACHE_AGE


# Download file from GCS
def download_from_gcs(folder: str, filename: str):
    os.makedirs(ASSET_DIR, exist_ok=True)   # creazione cartella "assets" in caso non esista

    # Creazione path
    local_path = os.path.join(ASSET_DIR, filename)
    gcs_path = f"{folder}/{filename}"      # NB: rispettare il format GCS per le directory (niente punto iniziale e barra finale)

    # Connessione al bucket
    blob = storage.Client().bucket(ASSET_BUCKET_NAME).blob(gcs_path)

    if blob.exists():
        blob.download_to_filename(local_path)   # download effettivo del file remoto
        print(f"File scaricato da GCS: {gcs_path} -> {local_path}")
    else:
        print(f"File non trovato su GCS: {gcs_path}")


# Upload file to GCS
def upload_to_gcs(folder: str, filename: str):
    # Creazione path
    local_path = os.path.join(ASSET_DIR, filename)
    gcs_path = f"{folder}/{filename}"      # NB: rispettare il format GCS per le directory (niente punto iniziale e barra finale)

    # Controllo esistenza file locale
    if not os.path.isfile(local_path):
        print(f"File locale non trovato: {local_path}")
        return

    # Connessione al bucket
    blob = storage.Client().bucket(ASSET_BUCKET_NAME).blob(gcs_path)

    # Caricamento dati su file remoto
    blob.upload_from_filename(local_path)
    print(f"File caricato su GCS: {local_path} -> {gcs_path}")


# Upload file with shared configuration constants
def upload_config(n):
    conf_obj = {
        "n_batches": n,
        "result_filename": RESULT_FILENAME,
        "asset_bucket_name": ASSET_BUCKET_NAME,
        "gcs_cache_dir": GCS_CACHE_DIR,
        "gcs_result_dir": GCS_RESULT_DIR,
        "max_concurrent_requests": MAX_CONCURRENT_REQUESTS,
        "max_cache_age": MAX_CACHE_AGE
    }

    blob = storage.Client().bucket(ASSET_BUCKET_NAME).blob("config.json")
    blob.upload_from_string(json.dumps(conf_obj, indent=2))
