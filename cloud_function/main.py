# CRF: Cloud Run Function

import os, json, posixpath

from google.cloud import storage
from resource_manager import resource_manager as res


# Merge single result files in one final 'result.json' (eseguita come Cloud Function al trigger di GCS, cioè ogni volta che un file 'result' viene creato)
def merge_handler(event, context):
    try:
        bucket_name = event['bucket']   # estrazione nome bucket da chi ha generato l'evento trigger
        bucket = storage.Client().bucket(bucket_name)
        
        # Estrazione dei batch result
        prefix = res.gcs_batch_result_dir + "/"
        blobs = list(bucket.list_blobs(prefix=prefix))
        n_blobs = len(blobs)

        if n_blobs == 0:
            res.logger.warning(f"[CRF][main][merge_handler] -> No files found under prefix '{prefix}'")
            return

        # Estrazione metadati
        dataset_name = os.path.basename(blobs[0].name).split("_result_")[0]     # nome dataset (presente in ognuno dei nomi dei file result)
        metadata_path = posixpath.join(res.gcs_dataset_dir, f"{dataset_name}_metadata.json")
        metadata_text = bucket.blob(metadata_path).download_as_text()
        metadata = json.loads(metadata_text)
        n_batches = metadata.get("num_batches", -1)

        if n_batches < 0:
            res.logger.warning("[CRF][main][merge_handler] -> 'n_batches' is undefined in the configuration file on GCS")
            return

        if n_blobs < n_batches:
            res.logger.debug(f"[CRF][main][merge_handler] -> Found only {n_blobs}/{n_batches} files")
            return
        
    except Exception as e:
        res.logger.error(f"[CRF][main][merge_handler] -> Error in section 1 ({type(e).__name__}): {str(e)}")
        raise

    res.logger.info(f"[CRF][main][merge_handler] -> Found {n_blobs}/{n_batches} files. Now merging")

    try:
        # Unione dei file result temporanei
        merged = []
        for blob in blobs:
            try:
                lines = blob.download_as_text().strip().splitlines()
                merged.extend(json.loads(line) for line in lines)
                
            except Exception as e:
                res.logger.warning(f"[CRF][main][merge_handler] -> Failed to read file '{blob.name}' ({type(e)}.__name__): {str(e)}")

        # Upload file unificato su GCS
        gcs_result_path = posixpath.join(res.gcs_result_dir, f"{dataset_name}_result.json")
        
        merged_blob = bucket.blob(gcs_result_path)
        merged_blob.upload_from_string(
            json.dumps(merged, indent=2),
            content_type="application/json"
        )

        res.logger.info(f"[CRF][main][merge_handler] -> {len(merged)} alerts saved in '{gcs_result_path}'")

        # Rimozione file temporanei
        count = 0
        for blob in blobs:
            blob.delete()
            count += 1

        res.logger.info(f"[CRF][main][merge_handler] -> {count} batch result files deleted")

    except Exception as e:
        res.logger.error(f"[CRF][main][merge_handler] -> Error in section 2 ({type(e).__name__}): {str(e)}")
        raise