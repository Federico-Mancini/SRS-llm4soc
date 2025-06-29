import os, json
from utils.resource_manager import ResourceManager


res = ResourceManager()


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


# Suddivisione del dataset su GCS in singoli batch
def split_dataset(dataset_filename: str):
    gcs_dataset_path = f"{res.gcs_dataset_dir}/{dataset_filename}"
    
    blob = res.bucket.blob(gcs_dataset_path)

    if not blob.exists():
        res.logger.error(f"[CRR][gcs_utils][split_dataset] File {gcs_dataset_path} not found in bucket {res.asset_bucket_name}")
        raise FileNotFoundError(f"File {gcs_dataset_path} not found in bucket {res.asset_bucket_name}")

    # Download e conteggio degli alert
    data = blob.download_as_text()
    lines = [line for line in data.splitlines() if line.strip()]
    total = len(lines)

    # Calcololo del numero di batch
    n_batches = max(1, (total + res.alerts_per_batch - 1) // res.alerts_per_batch)  # arrotondamento per eccesso
    res.logger.info(f"[CRR][gcs_utils][split_dataset] Splitting {total} alerts from {gcs_dataset_path} into {n_batches} batches")

    update_config_value("n_batches", n_batches)     # aggiornamento variabile d'ambiente condivisa su GCS

    # Parsing JSON e suddivisione dataset
    alerts = [json.loads(line) for line in lines]
    batch_size = max(1, total // n_batches)

    dataset_name = os.path.splitext(dataset_filename)[0]

    for i in range(n_batches):
        start = i * batch_size
        end = None if i == n_batches - 1 else (i + 1) * batch_size
        batch = alerts[start:end]

        out_path = f"{res.gcs_batch_dir}/{dataset_name}_batch_{i}.jsonl"
        content = "\n".join(json.dumps(entry) for entry in batch)
        res.bucket.blob(out_path).upload_from_string(content)

        res.logger.info(f"[CRR][gcs_utils][split_dataset] Batch {i} saved to {out_path} ({len(batch)} alerts)")

    return [f"{res.gcs_batch_dir}/{dataset_name}_batch_{i}.jsonl" for i in range(n_batches)]
