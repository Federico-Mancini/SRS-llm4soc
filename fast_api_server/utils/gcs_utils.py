import os, io, csv, json, posixpath

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


# Scrittura in append di nuova entry sul dataset CSV usato nell'addestrare del Linear Regressor
def append_to_training_dataset(new_row: dict) -> bool:
    blob = res.bucket.blob(res.ml_dataset_filename)

    rows = []
    fieldnames = list(new_row.keys())

    # Se il file esiste, legge il contenuto
    if blob.exists():
        try:
            content = blob.download_as_text()
            output = io.StringIO(content)
            reader = csv.DictReader(output)
            rows = list(reader)
            fieldnames = reader.fieldnames or fieldnames
        except Exception as e:
            msg = f"[VMS][gcs_utils][append_to_training_dataset] Failed to parse '{res.ml_dataset_filename}' to CSV ({type(e).__name__}): {str(e)}"
            res.logger.warning(msg)
            raise HTTPException(status_code=500, detail=msg)
    
    # Controllo duplicati
    new_row_str = {k: str(v) for k, v in new_row.items()}   # conversione valori entry in stringhe (necessario per confronto tra entry)
    for row in rows:
        if {k: str(v) for k, v in row.items()} == new_row_str:
            res.logger.info("[VMS][gcs_utils][append_to_training_dataset] Entry already exists in the training dataset")
            return False

    rows.append(new_row)
    
    # Scrittura contenuto aggiornato
    output = io.StringIO()
    writer = csv.DictWriter(output, fieldnames=fieldnames)
    writer.writeheader()
    writer.writerows(rows)

    # Caricamento su GCS
    blob.upload_from_string(output.getvalue(), content_type="text/csv")

    res.logger.info("[VMS][gcs_utils][append_to_training_dataset] New entry appended to training dataset")
    return True


# Lettura del dataset CV usato nell'addestramento del Linear Regressor
def read_training_dataset() -> list[dict]:
    blob = res.bucket.blob(res.ml_dataset_filename)

    if not blob.exists():
        msg = f"[VMS][gcs_utils][read_training_dataset] File '{res.ml_dataset_filename}' not found."
        res.logger.error(msg)
        raise HTTPException(status_code=404, detail=msg)

    try:
        content = blob.download_as_text()
        csv_io = io.StringIO(content)
        reader = csv.DictReader(csv_io)
        return list(reader)
    except Exception as e:
        msg = f"[VMS][gcs_utils][read_training_dataset] Failed to parse '{res.ml_dataset_filename}' to CSV ({type(e).__name__}): {str(e)}"
        res.logger.error(msg)
        raise HTTPException(status_code=500, detail=msg)


# Svuotamento directory remota
def empty_dir(gcs_dir: str):
    blobs = res.bucket.list_blobs(prefix=f"{gcs_dir}/")

    count = 0
    for blob in blobs:
        blob.delete()
        count += 1
    
    res.logger.info(f"[VMS][gcs_utils][empty_gcs_dir] {count} files deleted from '{gcs_dir}/'")
