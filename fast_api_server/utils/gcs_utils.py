import os, posixpath
import pandas as pd

from google.cloud import storage
from utils.resource_manager import resource_manager as res


# Svuotamento directory
def empty_dir(gcs_dir: str):
    blobs = res.bucket.list_blobs(prefix=f"{gcs_dir}/")

    count = 0
    for blob in blobs:
        blob.delete()
        count += 1
    
    res.logger.info(f"[VMS][gcs_utils][empty_gcs_dir] {count} files deleted from {gcs_dir}/")


# Calcolo metadati di un dataset remoto
def get_dataset_metadata(dataset_filename: str):
    gcs_dataset_path = posixpath.join(res.gcs_dataset_dir, dataset_filename)
    dataset_name, file_format = os.path.splitext(dataset_filename)

    # Download dati da GCS
    blob = res.bucket.blob(gcs_dataset_path)

    if not blob.exists():
        msg = f"[VMS][gcs_utils][get_dataset_metadata] -> File '{gcs_dataset_path}' not found in '{gcs_dataset_path}'"
        res.logger.warning(msg)
        raise FileNotFoundError(msg)

    data = blob.download_as_text()

    # Estrazione dati da file
    if file_format == '.csv':
        df = pd.read_csv(pd.compat.StringIO(data))
    elif file_format == '.jsonl':
        df = pd.read_json(pd.compat.StringIO(data), lines=True)
    else:
        msg = f"[VMS][gcs_utils][get_dataset_metadata] -> Invalid file format: {file_format} is not '.jsonl' or '.csv'"
        res.logger.warning(msg)
        raise ValueError(msg)

    # Conteggio batch da generare
    num_rows = df.shape[0]
    batch_size = res.alerts_per_batch
    n_batches = max(1, (num_rows + batch_size - 1) // batch_size)

    return {
        "num_rows": num_rows,
        "num_columns": df.shape[1],
        "features": df.columns.tolist(),
        "num_batches": n_batches,
        "batch_size": batch_size,
        "file_size_bytes": blob.size,
        "content_type": blob.content_type,
        "dataset_name": dataset_name,
        "dataset_path": gcs_dataset_path
    }