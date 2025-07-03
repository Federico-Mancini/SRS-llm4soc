import os, json

from fastapi import HTTPException
from utils.resource_manager import resource_manager as res


# Lettura file JSON locale
def read_local_json(local_path: str) -> list | dict:    # se nel file c'è un solo oggetto, sarà restituito un 'dict', altrimenti una 'list[dict]'
    with open(local_path, "r") as f:
        data = json.load(f)

    res.logger.info(f"[VMS][gcs_utils][read_local_json] -> File '{local_path}' read")
    return data


# Upload file locale in GCS
def upload_to_gcs(local_path: str, blob_path: str):
    try:
        blob = res.bucket.blob(blob_path)
        blob.upload_from_filename(local_path)
        res.logger.info(f"[VMS][gcs_utils][upload_to_gcs] Uploaded '{local_path}' to GCS as '{blob_path}'")
    except Exception as e:
        msg = f"[VMS][gcs_utils][upload_to_gcs] Failed to upload '{local_path}' to GCS ({type(e).__name__}): {str(e)}"
        res.logger.error(msg)
        raise HTTPException(status_code=500, detail=msg)


# Download file remoto in locale
def download_to_local(blob_path: str, local_path: str):
    blob = res.bucket.blob(blob_path)

    # Controllo esistenza file remoto
    if not blob.exists():
        msg = f"File '{blob_path}' not found"
        res.logger.error(msg)
        raise HTTPException(status_code=404, detail=msg)
        
    blob.download_to_filename(local_path)

    # Controllo esistenza file locale
    if not os.path.exists(local_path):
        msg = f"Downloaded file not found locally in '{local_path}'"
        res.logger.error(msg)
        raise HTTPException(status_code=404, detail=msg)
    
    res.logger.info(f"[VMS][io_utils][download_to_local] File '{blob_path}' downloaded to '{local_path}'")