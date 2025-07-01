import os, io, json, posixpath
import pandas as pd

from utils.resource_manager import resource_manager as res


# Svuotamento directory
def empty_gcs_dir(gcs_dir: str):
    blobs = res.bucket.list_blobs(prefix=f"{gcs_dir}/")

    count = 0
    for blob in blobs:
        blob.delete()
        count += 1
    
    res.logger.info(f"[CRR][gcs_utils][empty_gcs_dir] {count} files deleted from {gcs_dir}/")


# Aggiornamento valori di variabili d'ambiente salvate in GCS
def update_config_value(key: str, value):
    blob = res.bucket.blob(res.vms_config_filename)

    if not blob.exists():
        res.logger.error(f"[CRR][gcs_utils][update_config_value] File {res.confi} not found")
        raise FileNotFoundError(f"File {res.vms_config_filename} not found")
    
    config = json.loads(blob.download_as_text())
    config[key] = value

    blob.upload_from_string(json.dumps(config, indent=2))


def split_dataset(dataset_filename: str):
    # Connessione al bucket
    gcs_dataset_path = f"{res.gcs_dataset_dir}/{dataset_filename}"
    blob = res.bucket.blob(gcs_dataset_path)

    # Controllo esistenza file su GCS
    if not blob.exists():
        msg = f"[CRR][gcs_utils][split_dataset] -> File {gcs_dataset_path} not found in bucket {res.asset_bucket_name}"
        res.logger.error(msg)
        raise FileNotFoundError(msg)

    # Download e conteggio righe
    raw_data = blob.download_as_text()
    lines = [line for line in raw_data.splitlines() if line.strip()]
    total = len(lines)

    # Conteggio batch da generare
    alerts_per_batch = res.alerts_per_batch
    n_batches = max(1, (total + alerts_per_batch - 1) // alerts_per_batch)

    res.logger.info(f"[CRR][gcs_utils][split_dataset] -> Splitting {total} alerts from {gcs_dataset_path} into {n_batches} batches")

    # Aggiornamento campo 'n_batches' su 'config.json' di GCS
    update_config_value("n_batches", n_batches)

    dataset_name = os.path.splitext(dataset_filename)[0]
    batch_paths = []

    # Scrittura streaming batch per batch
    for i in range(n_batches):
        start = i * alerts_per_batch
        end = min(start + alerts_per_batch, total)
        batch_lines = lines[start:end]
        
        buffer = io.StringIO("\n".join(batch_lines))

        out_path = f"{res.gcs_batch_dir}/{dataset_name}_batch_{i}.jsonl"
        res.bucket.blob(out_path).upload_from_file(buffer, rewind=True, content_type="application/jsonl")

        batch_paths.append(out_path)

    res.logger.info(f"[CRR][gcs_utils][split_dataset] -> Generation of {n_batches} batches completed")
    return batch_paths


# Calcolo metadati di un dataset remoto
def get_dataset_metadata(dataset_filename: str):
    gcs_dataset_path = posixpath.join(res.gcs_dataset_dir, dataset_filename)
    dataset_name, file_format = os.path.splitext(dataset_filename)

    # Download dati da GCS
    blob = res.bucket.blob(gcs_dataset_path)

    if not blob.exists():
        msg = f"[CRR][gcs_utils][get_dataset_metadata] -> File '{gcs_dataset_path}' not found in '{gcs_dataset_path}'"
        res.logger.warning(msg)
        raise FileNotFoundError(msg)

    data = blob.download_as_text()

    # Estrazione dati da file
    if file_format == '.csv':
        df = pd.read_csv(pd.compat.StringIO(data))
    elif file_format == '.jsonl':
        df = pd.read_json(pd.compat.StringIO(data), lines=True)
    else:
        msg = f"[CRR][gcs_utils][get_dataset_metadata] -> Invalid file format: {file_format} is not '.jsonl' or '.csv'"
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