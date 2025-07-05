import io, json, csv, posixpath

from google.cloud import storage
from utils.resource_manager import resource_manager as res


# Estrazione metadati di dataset pre-caricato su GCS
def get_metadata(bucket: storage.Bucket, dataset_name: str) -> dict:
    metadata_path = posixpath.join(res.gcs_dataset_dir, f"{dataset_name}_metadata.json")
    metadata_text = bucket.blob(metadata_path).download_as_text()
    return json.loads(metadata_text)


# Estrazione dati da file multipli per creare uno stream di entry JSONL
def stream_jsonl_blobs(blobs: list[storage.Blob]):
    for blob in blobs:
        try:
            lines = blob.download_as_text().strip().splitlines()
            for line in lines:
                yield json.loads(line)
        except Exception as e:
            res.logger.error(f"[CRF][merge_utils][stream_jsonl_blobs] -> Error in '{blob.name}': {str(e)}")


# Upload JSONL files as one JSON
def upload_json(bucket: storage.Bucket, path: str, data_gen):
    blob = bucket.blob(path)
    data = list(data_gen)
    blob.upload_from_string(
        json.dumps(data, indent=2),
        content_type="application/json"
    )


# Aggiorna il file CSV aggiungendo in append i nuovi dati
def update_csv(bucket: storage.Bucket, path: str, data_gen):
    blob = bucket.blob(path)
    data = list(data_gen)
    
    if not data:
        res.logger.info(f"[CRF][merge_utils][upload_csv_append] -> No data to append for '{path}'")
        return

    fieldnames = sorted(data[0].keys())
    existing_rows, header_exists = [], False

    try:
        if blob.exists():
            reader = csv.DictReader(io.StringIO(blob.download_as_text()))
            existing_rows = list(reader)
            header_exists = bool(reader.fieldnames)
        else:
            res.logger.info(f"[CRF][gcs_utils][update_csv] -> Blob '{path}' does not exist. Creating new file with header.")
    except Exception as e:
        res.logger.warning(f"[CRF][gcs_utils][update_csv] -> Failed to read existing data ({type(e).__name__}): {e}")

    output_io = io.StringIO()
    writer = csv.DictWriter(output_io, fieldnames=fieldnames)
    
    if not header_exists:
        writer.writeheader()
    writer.writerows(existing_rows + data)

    blob.upload_from_string(output_io.getvalue(), content_type="text/csv")
    res.logger.info(f"[CRF][gcs_utils][upload_csv_append] -> Appended {len(data)} rows to '{path}'")
