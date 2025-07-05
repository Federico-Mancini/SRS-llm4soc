# CRF: Cloud Run Function

import os, posixpath
import utils.gcs_utils as gcs

from google.cloud import storage
from utils.resource_manager import resource_manager as res


# Merge single result files in one final 'result.json' (eseguita come Cloud Function al trigger di GCS, cioè ogni volta che un file 'result' viene creato)
def merge_handler(event, context):
    # Filtra solo eventi legati ai result batch (complemento al trigger con osservabilità limitata all'intero bucket)
    results_prefix = res.gcs_batch_result_dir + "/"
    metrics_prefix = res.gcs_batch_metrics_dir + "/"

    try:
        # Parametri dell'origine dell'evento trigger
        bucket_name = event["bucket"]
        object_name = event["name"]

        if not object_name.startswith(results_prefix):
            return
        
        # Estrazione file (risultati e metriche)
        bucket = storage.Client().bucket(bucket_name)
        res_blobs = list(bucket.list_blobs(prefix=results_prefix))
        met_blobs = list(bucket.list_blobs(prefix=metrics_prefix))
    
        if not res_blobs:           # assenza di file nella directory, 
            return
        if gcs.check_stop_flag(bucket):  # presenza del flag d'interruzione (utile quando ci sono già gli N file che ci si aspetta ma il merge è già stato eseguito)
            return

        try:
            # Lettura metadati
            dataset_name = os.path.basename(res_blobs[0].name).split("_result_")[0]     # es: "ABC_result_0.jsonl" -> "ABC"
            metadata = gcs.get_metadata(bucket, dataset_name)
        except IndexError:
            return
        except Exception as e:
            res.logger.error(f"[CRF][main][merge_handler] -> Failed to retrieve metadata ({type(e).__name__}): {str(e)}")
            raise

        expected_batches = metadata.get("num_batches", -1)
        if expected_batches < 0:
            res.logger.warning("[CRF][main][merge_handler] -> 'n_batches' undefined in metadata")
            return
        
        n_blobs = len(res_blobs)
        if n_blobs < expected_batches:
            res.logger.info(f"[CRF][main][merge_handler] -> Found only {n_blobs}/{expected_batches} batch result files")
            return
        
        gcs_result_path = posixpath.join(res.gcs_result_dir, f"{dataset_name}_result.json")
        gcs_metrics_path = posixpath.join(res.gcs_metrics_dir, f"{dataset_name}_metrics.json")
        gcs_metrics_csv_path = posixpath.join(res.gcs_metrics_dir, f"{dataset_name}_metrics.csv")

        # Unificazione e upload file JSON (batch result file)
        res.logger.info(f"[CRF][main][merge_handler] -> Saving {n_blobs} batch result files in '{gcs_result_path}'")
        result_data = list(gcs.stream_jsonl_blobs(res_blobs))
        gcs.upload_json(bucket, gcs_result_path, result_data)

        # Unificazione e upload file CSV (batch metrics file)
        res.logger.info(f"[CRF][main][merge_handler] -> Saving {n_blobs} batch metrics files in '{gcs_metrics_path}'")
        metrics_data = list(gcs.stream_jsonl_blobs(met_blobs))
        gcs.upload_json(bucket, gcs_metrics_path, metrics_data)
        gcs.update_csv(bucket, gcs_metrics_csv_path, metrics_data)

        gcs.set_stop_flag(bucket)

    except Exception as e:
        res.logger.error(f"[CRF][main][merge_handler] -> Error ({type(e).__name__}): {str(e)}")
        raise