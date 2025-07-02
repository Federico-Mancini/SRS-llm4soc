import io, json, csv

from typing import List
from google.cloud import storage
from utils.resource_manager import resource_manager as res


# Unione file JSONL (batch result file)
def stream_jsonl_blobs(blobs: List[storage.Blob]):
    for blob in blobs:
        try:
            lines = blob.download_as_text().strip().splitlines()
            for line in lines:
                yield json.loads(line)
        except Exception as e:
            res.logger.error(f"[CRF][merge_utils][stream_jsonl_blobs] -> Error in '{blob.name}': {str(e)}")


# Unione file CSV (metrics file)
def merge_csv_blobs(blobs: List[storage.Blob]) -> list:
    merged = []
    for blob in blobs:
        try:
            content = blob.download_as_text()
            reader = csv.DictReader(io.StringIO(content))
            merged.extend(row for row in reader)
        except Exception as e:
            print(f"[merge_csv_blobs] Error in {blob.name}: {e}")
    return merged
