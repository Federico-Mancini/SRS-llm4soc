import os

from google.cloud import storage
from utils.resource_manager import resource_manager as res


# Download file remoti
def download_from(folder: str, filename: str):
    os.makedirs("assets", exist_ok=True)   # creazione cartella "assets" in caso non esista

    # Creazione path
    local_path = os.path.join("assets", filename)
    gcs_path = f"{folder}/{filename}" if folder else filename   # NB: rispettare il format GCS per le directory (niente punto iniziale e barra finale)

    # Connessione al bucket
    blob = storage.Client().bucket(res.asset_bucket_name).blob(gcs_path)

    if blob.exists():
        blob.download_to_filename(local_path)   # download effettivo del file remoto
        res.logger.info(f"[VMS][gcs_utils][download_from_gcs] File {gcs_path} downloaded into {local_path}")
    else:
        res.logger.info(f"[VMS][gcs_utils][download_from_gcs] File {gcs_path} not found")


# Upload file locali
def upload_to(folder: str, filename: str):
    # Creazione path
    local_path = os.path.join("assets", filename)
    gcs_path = f"{folder}/{filename}" if folder else filename   # NB: rispettare il format GCS per le directory (niente punto iniziale e barra finale)

    # Controllo esistenza file locale
    if not os.path.isfile(local_path):
        res.logger.error(f"[VMS][gcs_utils][upload_to_gcs] File {local_path} not found")
        return
    
    # Caricamento dati su file remoto
    blob = res.bucket.blob(gcs_path)
    blob.upload_from_filename(local_path)
    res.logger.info(f"[VMS][gcs_utils][upload_to_gcs] File {local_path} uploaded to {gcs_path}")
