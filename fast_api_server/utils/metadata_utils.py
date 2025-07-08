# Metadata Utils: modulo per la gestione dei metadata associati ai dataset analizzati

import os, io, posixpath
import pandas as pd
import utils.gcs_utils as gcs

from utils.resource_manager import resource_manager as res


# F01 - Calcolo metadati di un dataset remoto
def create_metadata(dataset_filename: str) -> dict:
    gcs_dataset_path = posixpath.join(res.gcs_dataset_dir, dataset_filename)
    dataset_name, file_format = os.path.splitext(dataset_filename)

    # Download dati da GCS
    blob = res.bucket.blob(gcs_dataset_path)

    if not blob.exists():
        msg = f"[metadata|F01]\t -> File '{gcs_dataset_path}' not found"
        res.logger.error(msg)
        raise FileNotFoundError(msg)

    data = blob.download_as_text()

    # Estrazione dati da file
    if file_format == '.csv':
        df = pd.read_csv(io.StringIO(data))
    elif file_format == '.json':
        df = pd.read_json(io.StringIO(data))
    elif file_format == '.jsonl':
        df = pd.read_json(io.StringIO(data), lines=True)
    else:
        msg = f"[metadata|F01]\t-> Invalid file format: '{file_format}' is not '.jsonl' or '.csv'"
        res.logger.warning(msg)
        raise ValueError(msg)

    return {
        "dataset_name": dataset_name,
        "dataset_path": gcs_dataset_path,
        "num_rows": df.shape[0],
        "num_columns": df.shape[1],
        "features": df.columns.tolist(),
        "content_type": blob.content_type
    }


# F02 - Download metadati da file remoto
def download_metadata(dataset_filename: str) -> dict:
    path = gcs.get_blob_path(res.gcs_dataset_dir, dataset_filename, "metadata", "json")
    return gcs.read_json(path)


# F03 - Upload metadati su file remoto
def upload_metadata(dataset_filename: str, metadata: dict):
    path = gcs.get_blob_path(res.gcs_dataset_dir, dataset_filename, "metadata", "json")
    gcs.write_json(metadata, path)