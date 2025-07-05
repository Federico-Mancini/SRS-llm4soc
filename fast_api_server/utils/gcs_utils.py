# GCS Utils: modulo per la lettura/scrittura di file remoti e il download/upload

import os, io, csv, json, posixpath

from fastapi import HTTPException
from utils.resource_manager import resource_manager as res


# F01 - Costruzione path remoto (usato in VMS per i file 'result', 'metrics' e 'metadata')
def get_blob_path(folder: str, dataset_filename: str, suffix: str, file_format: str) -> str:
    dataset_name = os.path.splitext(dataset_filename)[0]
    return posixpath.join(folder, f"{dataset_name}_{suffix}.{file_format}")


# F02 - Lettura file JSON remoto
def read_json(blob_path: str) -> list | dict:    # se nel file c'è un solo oggetto, sarà restituito un 'dict', altrimenti una 'list[dict]'
    blob = res.bucket.blob(blob_path)

    if not blob.exists():
        msg = f"[gcs|F02]\t\t-> Remote file '{blob_path}' not found"
        res.logger.error(msg)
        raise FileNotFoundError(msg)
    
    try:
        #return json.loads(blob.download_as_bytes())    # TODO: vecchio codice, da eliminare dopo aver testato il nuovo
        data = json.loads(blob.download_as_text())
    except Exception as e:
        msg = f"[gcs|F02]\t\t-> Failed to read remote JSON in '{blob_path}' ({type(e).__name__}): {str(e)}"
        res.logger.error(msg)
        raise HTTPException(status_code=500, detail=msg)
    
    res.logger.info(f"[gcs|F02]\t\t-> Remote file read from '{blob_path}'")
    return data

# F03 - Scrittura file JSON in remoto
def write_json(data: dict, blob_path: str):
    try:
        blob = res.bucket.blob(blob_path)
        blob.upload_from_string(
            json.dumps(data, indent=2),
            content_type='application/json'
        )
    except Exception as e:
        msg = f"[gcs|F03]\t\t-> Failed to write local JSON to '{blob_path}' ({type(e).__name__}): {str(e)}"
        res.logger.error(msg)
        raise HTTPException(status_code=500, detail=msg)
    
    res.logger.info(f"[gcs|F03]\t\t-> Local file JSON written to '{blob_path}'")


# F04 - Download file remoto in locale
def download_to(blob_path: str, local_path: str):
    blob = res.bucket.blob(blob_path)
    if not blob.exists():
        msg = f"[gcs|F04]\t\t-> Remote file '{blob_path}' not found"
        res.logger.error(msg)
        raise FileNotFoundError(msg)
        
    try:
        blob.download_to_filename(local_path)
    except Exception as e:
        msg = f"[gcs|F04]\t\t-> Failed to download '{blob_path}' to '{local_path}' ({type(e).__name__}): {str(e)}"
        res.logger.error(msg)
        raise HTTPException(status_code=500, detail=msg)

    res.logger.info(f"[gcs|F04]\t\t-> Remote file '{blob_path}' downloaded to '{local_path}'")

# F05 - Upload file locale in GCS
def upload_to(local_path: str, blob_path: str):
    if not os.path.exists(local_path):
        msg = f"[gcs|F05]\t\t-> Local file '{local_path}' not found"
        res.logger.error(msg)
        raise FileNotFoundError(msg)

    try:
        blob = res.bucket.blob(blob_path)
        blob.upload_from_filename(local_path)
    except Exception as e:
        msg = f"[gcs|F05]\t\t-> Failed to upload '{local_path}' to '{blob_path}' ({type(e).__name__}): {str(e)}"
        res.logger.error(msg)
        raise HTTPException(status_code=500, detail=msg)

    res.logger.info(f"[gcs|F05]\t\t-> Local file '{local_path}' uploaded to '{blob_path}'")


# F06 - Svuotamento directory remota
def empty_dir(gcs_dir: str):
    blobs = res.bucket.list_blobs(prefix=f"{gcs_dir}/")
    count = 0
    for blob in blobs:
        blob.delete()
        count += 1
    res.logger.info(f"[gcs|F06]\t\t-> {count} files deleted from '{gcs_dir}/'")


# F07 - Scrittura in append di nuova entry sul dataset CSV usato nell'addestrare del Linear Regressor
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
            msg = f"[gcs|F07]\t\t-> Failed to parse '{res.ml_dataset_filename}' to CSV ({type(e).__name__}): {str(e)}"
            res.logger.warning(msg)
            raise HTTPException(status_code=500, detail=msg)
    
    # Controllo duplicati
    new_row_str = {k: str(v) for k, v in new_row.items()}   # conversione valori entry in stringhe (necessario per confronto tra entry)
    for row in rows:
        if {k: str(v) for k, v in row.items()} == new_row_str:
            res.logger.info("[gcs|F07]\t\t-> Entry already exists in the training dataset")
            return False

    rows.append(new_row)
    
    # Scrittura contenuto aggiornato
    output = io.StringIO()
    writer = csv.DictWriter(output, fieldnames=fieldnames)
    writer.writeheader()
    writer.writerows(rows)

    # Caricamento su GCS
    blob.upload_from_string(output.getvalue(), content_type="text/csv")

    res.logger.info("[gcs|F07]\t\t-> New entry appended to training dataset")
    return True

# F08 - Lettura del dataset CV usato nell'addestramento del Linear Regressor
def read_training_dataset() -> list[dict]:
    blob = res.bucket.blob(res.ml_dataset_filename)

    if not blob.exists():
        msg = f"[gcs|F08]\t\t-> File '{res.ml_dataset_filename}' not found."
        res.logger.error(msg)
        raise HTTPException(status_code=404, detail=msg)

    try:
        content = blob.download_as_text()
        csv_io = io.StringIO(content)
        reader = csv.DictReader(csv_io)
        return list(reader)
    except Exception as e:
        msg = f"[gcs|F08]\t\t-> Failed to parse '{res.ml_dataset_filename}' to CSV ({type(e).__name__}): {str(e)}"
        res.logger.error(msg)
        raise HTTPException(status_code=500, detail=msg)


# F09 - Conversione file metriche JSONL in CSV
def convert_metrics_json_to_csv(gcs_json_path: str):
    try:
        blob = res.bucket.blob(gcs_json_path)
        content = blob.download_as_text()
        data = json.loads(content)

        if not isinstance(data, list) or not data:
            res.logger.warning(f"[gcs|F09]\t\t-> JSON non valido o vuoto in '{gcs_json_path}'")
            return

    except Exception as e:
        res.logger.error(f"[gcs|F09]\t\t-> Errore lettura/parsing file '{gcs_json_path}': {type(e).__name__} - {str(e)}")
        raise

    dest_path = os.path.splitext(gcs_json_path)[0] + ".csv"

    output = io.StringIO()
    keys = data[0].keys()
    writer = csv.DictWriter(output, fieldnames=keys)
    writer.writeheader()
    writer.writerows(data)

    res.bucket.blob(dest_path).upload_from_string(output.getvalue(), content_type="text/csv")

    res.logger.info(f"[gcs|F09]\t\t-> JSON '{gcs_json_path}' converted into CSV '{dest_path}'")
