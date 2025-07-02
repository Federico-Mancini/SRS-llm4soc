import os, json, posixpath

from fastapi import HTTPException
from utils.resource_manager import resource_manager as res


# Costruzione path remoto (usato in VMS per file 'result', 'metrics' e 'metadata')
def get_blob_path(folder: str, dataset_filename: str, suffix: str, file_format: str) -> str:
    dataset_name = os.path.splitext(dataset_filename)[0]
    return posixpath.join(folder, f"{dataset_name}_{suffix}.{file_format}")


# Lettura file JSON remoto
def read_json(blob_path: str):
    blob = res.bucket.blob(blob_path)

    # Controllo esistenza file remoto
    if not blob.exists():
        msg = f"File '{blob_path}' not found"
        res.logger.error(msg)
        raise HTTPException(status_code=404, detail=msg)
    
    json_bytes = blob.download_as_bytes()
    return json.loads(json_bytes)
        

# Svuotamento directory remota
def empty_dir(gcs_dir: str):
    blobs = res.bucket.list_blobs(prefix=f"{gcs_dir}/")

    count = 0
    for blob in blobs:
        blob.delete()
        count += 1
    
    res.logger.info(f"[VMS][gcs_utils][empty_gcs_dir] {count} files deleted from '{gcs_dir}/'")
