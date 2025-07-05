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
        res.logger.info(f"[CRF][gcs_utils][update_csv] -> No data to append for '{path}'")
        return

    fieldnames = sorted(data[0].keys())
    output_io = io.StringIO()
    writer = csv.DictWriter(output_io, fieldnames=fieldnames)

    try:
        if blob.exists():
            output_io.write(blob.download_as_text())    # scrittura di dati pre-esistenti
    except Exception as e:
        res.logger.warning(f"[CRF][gcs_utils][update_csv] -> Failed to read existing CSV ({type(e).__name__}): {str(e)}")

    for row in data:
        writer.writerow(row)    # scrittura di nuovi dati

    blob.upload_from_string(output_io.getvalue(), content_type="text/csv")
    res.logger.info(f"[CRF][gcs_utils][update_csv] -> Appended {len(data)} rows (no header) to '{path}'")
