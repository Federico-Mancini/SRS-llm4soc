# CRF: Cloud Run Function

import os, json, posixpath

from google.cloud import storage
from resource_manager import resource_manager as res


# Merge single result files in one final 'result.json' (eseguita come Cloud Function al trigger di GCS, cioè ogni volta che un file 'result' viene creato)
def merge_handler(event, context):
    prefix = res.gcs_batch_result_dir + "/"     # prefisso file "interessanti" (i trigger osservano l'intero bucket, non esiste un filtro più restrittivo)

    try:
        # Estrazione sorgenti evento
        bucket_name = event['bucket']
        object_name = event["name"]

        if not object_name.startswith(prefix):
            return
        
        # Estrazione dei batch result
        bucket = storage.Client().bucket(bucket_name)
        blobs = list(bucket.list_blobs(prefix=prefix))
    
        if not blobs:
            return

        # Estrazione metadati
        try:
            dataset_name = os.path.basename(blobs[0].name).split("_result_")[0]     # es: "ABC_result_0.jsonl" -> "ABC"
            metadata_blob = bucket.blob(posixpath.join(res.gcs_dataset_dir, f"{dataset_name}_metadata.json"))
            metadata = json.loads(metadata_blob.download_as_text())
            
        except IndexError:
            return  # nessun log in quanto è un errore senza effetti negativi, legato all'esecuzione parallela dell CRF

        except Exception as e:
            res.logger.error(f"[CRF][main][merge_handler] -> Failed to retrieve metadata ({type(e).__name__}): {str(e)}")
            raise

        expected_batches = metadata.get("num_batches", -1)
        if expected_batches < 0:
            res.logger.warning("[CRF][main][merge_handler] -> 'n_batches' is undefined in the configuration file on GCS")
            return
        
        n_blobs = len(blobs)
        if n_blobs < expected_batches:
            res.logger.info(f"[CRF][main][merge_handler] -> Found only {n_blobs}/{expected_batches} files")
            return
        
        res.logger.info(f"[CRF][main][merge_handler] -> Merging {n_blobs} batch result files")

        # Unione dei file result temporanei
        merged_alerts = []
        for blob in blobs:
            try:
                lines = blob.download_as_text().strip().splitlines()
                merged_alerts.extend(json.loads(line) for line in lines)
                
            except Exception as e:
                res.logger.error(f"[CRF][main][merge_handler] -> Failed to parse '{blob.name}' ({type(e)}.__name__): {str(e)}")

        # Upload file unificato su GCS
        gcs_result_path = posixpath.join(res.gcs_result_dir, f"{dataset_name}_result.json")
        
        merged_blob = bucket.blob(gcs_result_path)
        merged_blob.upload_from_string(
            json.dumps(merged_alerts, indent=2),
            content_type="application/json"
        )

        res.logger.info(f"[CRF][main][merge_handler] -> {len(merged_alerts)} alerts saved in '{gcs_result_path}'")

    except Exception as e:
        res.logger.error(f"[CRF][main][merge_handler] -> Error ({type(e).__name__}): {str(e)}")
        raise