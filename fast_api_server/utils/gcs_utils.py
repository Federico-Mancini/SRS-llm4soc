# GCS Utils: modulo per la lettura/scrittura di file remoti e il download/upload

import os, io, time, csv, json, posixpath

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


# F09 - Rimozione entry duplicate da CSV contenenti le metriche passate dei dataset
def remove_duplicate_rows():
    blobs = list(res.bucket.list_blobs(prefix=res.gcs_metrics_dir))
    
    for blob in blobs:
        if not blob.name.endswith(".csv"):
            continue

        data = blob.download_as_text()

        input_io = io.StringIO(data)
        output_io = io.StringIO()
        reader = csv.reader(input_io)
        writer = csv.writer(output_io)
        
        seen = set()
        for row in reader:
            row_tuple = tuple(row)
            if row_tuple not in seen:
                seen.add(row_tuple)
                writer.writerow(row)

        blob.upload_from_string(output_io.getvalue(), content_type='text/csv')
        res.logger.info(f"[GCS][remove_duplicates_in_dir] -> Cleaned duplicates in '{blob.name}'")
        time.sleep(3)   # attesa necessaria per ridurre l'overhead (altrimenti, errore 429)
