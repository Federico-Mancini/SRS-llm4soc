# CRF: Cloud Run Function

import os, time, posixpath
import utils.gcs_utils as gcs

from google.cloud import storage
from utils.resource_manager import resource_manager as res
from utils.lock_utils import acquire_lock


# F01 - Merge single result files in one final 'result.json' (eseguita come Cloud Function al trigger di GCS, cioè ogni volta che un file 'result' viene creato)
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
        
        bucket = storage.Client().bucket(bucket_name)

        # Estrazione file (risultati e metriche)
        res_blobs = list(bucket.list_blobs(prefix=results_prefix))
        met_blobs = list(bucket.list_blobs(prefix=metrics_prefix))
    
        if not res_blobs:   # assenza di file nella directory
            return

        try:
            # Lettura metadati
            dataset_name = os.path.basename(res_blobs[0].name).split("_result_")[0]     # es: "ABC_result_0.jsonl" -> "ABC"
            metadata = gcs.get_metadata(bucket, dataset_name)
        except IndexError:
            return
        except Exception as e:
            res.logger.error(f"[main|F01]\t\t-> Failed to retrieve metadata ({type(e).__name__}): {str(e)}")
            raise

        expected_batches = metadata.get("num_batches", -1)
        if expected_batches < 0:
            res.logger.warning("[main|F01]\t\t-> 'n_batches' undefined in metadata")
            return
        
        n_blobs = len(res_blobs) + len(met_blobs)
        if n_blobs < expected_batches * 2:
            res.logger.info(f"[main|F01]\t\t-> Found only {n_blobs}/{expected_batches*2} batch result files")
            return
        
        # Acquisizione lock (creazione flag)
        if not acquire_lock(bucket):
            return
        
        gcs_result_path = posixpath.join(res.gcs_result_dir, f"{dataset_name}_result.json")
        gcs_metrics_path = posixpath.join(res.gcs_metrics_dir, f"{dataset_name}_metrics.json")
        gcs_metrics_csv_path = posixpath.join(res.gcs_metrics_dir, f"{dataset_name}_metrics.csv")

        # Unificazione e upload file JSON (batch result file)
        res.logger.info(f"[main|F01]\t\t-> Saving {n_blobs} batch result files in '{gcs_result_path}'")
        result_data = list(gcs.stream_jsonl_blobs(res_blobs))
        gcs.upload_json(bucket, gcs_result_path, result_data)

        time.sleep(1)

        # Unificazione e upload file CSV (batch metrics file)
        res.logger.info(f"[main|F01]\t\t-> Saving {n_blobs} batch metrics files in '{gcs_metrics_path}'")
        metrics_data = list(gcs.stream_jsonl_blobs(met_blobs))
        gcs.upload_json(bucket, gcs_metrics_path, metrics_data)
        gcs.update_csv(bucket, gcs_metrics_csv_path, metrics_data)

    except Exception as e:
        res.logger.error(f"[main|F01]\t\t-> Error ({type(e).__name__}): {str(e)}")
        raise
