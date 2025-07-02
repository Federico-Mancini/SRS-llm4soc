import io, json, csv, posixpath

from google.cloud import storage
from utils.resource_manager import resource_manager as res


# Estrazione metadati di dataset pre-caricato su GCS
def get_metadata(bucket: storage.Bucket, dataset_name: str) -> dict:
    metadata_path = posixpath.join(res.gcs_dataset_dir, f"{dataset_name}_metadata.json")
    metadata_text = bucket.blob(metadata_path).download_as_text()
    return json.loads(metadata_text)


# Upload file JSON
def upload_jsonl_data(bucket: storage.Bucket, path: str, data_gen):
    blob = bucket.blob(path)    
    data = list(data_gen)
    blob.upload_from_string(
        json.dumps(data, indent=2),
        content_type="application/json"
    )


# Upload file CSV
def upload_csv_data(bucket: storage.Bucket, path: str, data: list, fieldnames: list):
    blob = bucket.blob(path)
    output = io.StringIO()
    writer = csv.DictWriter(output, fieldnames=fieldnames)
    writer.writeheader()

    for row in data:
        writer.writerow(row)

    blob.upload_from_string(output.getvalue(), content_type="text/csv")
    output.close()
