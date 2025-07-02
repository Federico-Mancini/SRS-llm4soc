import json, posixpath

from typing import List
from google.cloud import storage
from utils.resource_manager import resource_manager as res


# Estrazione metadati di dataset pre-caricato su GCS
def get_metadata(bucket: storage.Bucket, dataset_name: str) -> dict:
    metadata_path = posixpath.join(res.gcs_dataset_dir, f"{dataset_name}_metadata.json")
    metadata_text = bucket.blob(metadata_path).download_as_text()
    return json.loads(metadata_text)


# Estrazione dati da file multipli per creare uno stream di entry JSONL
def stream_jsonl_blobs(blobs: List[storage.Blob]):
    for blob in blobs:
        try:
            lines = blob.download_as_text().strip().splitlines()
            for line in lines:
                yield json.loads(line)
        except Exception as e:
            res.logger.error(f"[CRF][merge_utils][stream_jsonl_blobs] -> Error in '{blob.name}': {str(e)}")


def stream_jsonl_blobs_2(blobs: List[storage.Blob]):
    for blob in blobs:
        try:
            text = blob.download_as_text().strip()
            print(f"CRF - reading from blob '{blob.name}':\n{text}\n---")

            try:
                # Prova a caricare l'intero contenuto come JSON unico
                data = json.loads(text)
                if isinstance(data, list):
                    for item in data:
                        yield item
                else:
                    yield data
            except json.JSONDecodeError:
                # Fallback a JSONL per riga
                for i, line in enumerate(text.splitlines(), start=1):
                    line = line.strip()
                    if line:
                        try:
                            yield json.loads(line)
                        except json.JSONDecodeError as e:
                            res.logger.warning(f"[CRF - stream_jsonl_blobs] -> Linea non valida nel blob '{blob.name}' (riga {i}): {line} ({str(e)})")

        except Exception as e:
            res.logger.error(f"[CRF - stream_jsonl_blobs] -> Errore nel blob '{blob.name}': {str(e)}")



# Upload JSONL files as one JSON
def upload_json(bucket: storage.Bucket, path: str, data_gen):
    blob = bucket.blob(path)
    print(f"CRF - data_gen: {data_gen}")
    data = list(data_gen)
    print(f"CRF - data: {data}")
    blob.upload_from_string(
        json.dumps(data, indent=2),
        content_type="application/json"
    )
