# CRF: Cloud Run Function

import json

from google.cloud import storage
from resource_manager import resource_manager as res


# Merge single result files in one final 'result.json' (eseguita come Cloud Function al trigger di GCS, cioè ogni volta che un file 'result' viene creato)
def merge_handler(event, context):
    # Connessione al bucket
    bucket_name = event['bucket']   # estrazione nome bucket da chi ha generato l'evento trigger
    bucket = storage.Client().bucket(bucket_name)
    blobs = list(bucket.list_blobs(prefix=f"{res.gcs_batch_result_dir}/"))

    # Controllo presenza di tutti i file attesi
    if len(blobs) < res.n_batches:
        res.logger.info(f"[CRF][main][merge_handler] Found only {len(blobs)}/{res.n_batches} files. Waiting for others...")
        return

    # Unione dei file 'result'
    merged = []
    for blob in blobs:
        try:
            data = json.loads(blob.download_as_text())
            if isinstance(data, list):
                merged.extend(data)
        except Exception as e:
            res.logger.warning(f"[CRF][main][merge_handler] Failed to read file {blob.name} ({type(e)}): {str(e)}")

    dataset_name = blobs[0].name.split("_result-")[0]
    gcs_result_path = f"{res.gcs_result_dir}/{dataset_name}_result.json"
    merged_blob = bucket.blob(gcs_result_path)
    merged_blob.upload_from_string(json.dumps(merged, indent=2))

    res.logger.info(f"[CRF][main][merge_handler] {len(merged)} alerts saved in {gcs_result_path}")

    # Rimozione singoli file 'result' (non più necessari)
    for blob in blobs:
        blob.delete()

    res.logger.info(f"[CRF][main][merge_handler] Files deleted successfully")