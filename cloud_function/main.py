# CRF: Cloud Run Function

import os, posixpath
import utils.gcs_utils as gcs
import utils.merge_utils as mrg

from google.cloud import storage
from utils.resource_manager import resource_manager as res


# Merge single result files in one final 'result.json' (eseguita come Cloud Function al trigger di GCS, cioè ogni volta che un file 'result' viene creato)
def merge_handler(event, context):
    # prefissi file "interessanti"
    # NB: usati come filtro aggiuntivo come complemento dei trigger (essi possono osservare i cambiamenti a livello bucket ma non al di sotto di esso, cioè le singole cartelle)
    results_prefix = res.gcs_batch_result_dir + "/"
    metrics_prefix = res.gcs_metrics_dir + "/"

    try:
        # Parametri dell'origine dell'evento trigger
        bucket_name = event['bucket']
        object_name = event["name"]

        if not object_name.startswith(results_prefix):
            return
        
        # Estrazione file (risultati e metriche)
        bucket = storage.Client().bucket(bucket_name)
        res_blobs = list(bucket.list_blobs(prefix=results_prefix))
        met_blobs = list(bucket.list_blobs(prefix=metrics_prefix))
    
        if not res_blobs:
            return

        # Estrazione metadati
        try:
            dataset_name = os.path.basename(res_blobs[0].name).split("_result_")[0]     # es: "ABC_result_0.jsonl" -> "ABC"
            metadata = gcs.get_metadata(bucket, dataset_name)
            
        except IndexError:
            return  # nessun log in quanto è un errore senza effetti negativi, legato all'esecuzione parallela dell CRF

        except Exception as e:
            res.logger.error(f"[CRF][main][merge_handler] -> Failed to retrieve metadata ({type(e).__name__}): {str(e)}")
            raise

        expected_batches = metadata.get("num_batches", -1)
        if expected_batches < 0:
            res.logger.warning("[CRF][main][merge_handler] -> 'n_batches' is undefined in the configuration file on GCS")
            return
        
        n_blobs = len(res_blobs)
        if n_blobs < expected_batches:
            res.logger.info(f"[CRF][main][merge_handler] -> Found only {n_blobs}/{expected_batches} files")
            return
        
        # Unificazione e upload file JSON (batch result file)
        res.logger.info(f"[CRF][main][merge_handler] -> Saving {n_blobs} batch result files in '{gcs_result_path}'")
        result_generator = mrg.stream_jsonl_blobs(res_blobs)
        gcs_result_path = posixpath.join(res.gcs_result_dir, f"{dataset_name}_result.json")
        gcs.upload_jsonl_data(
            bucket,
            gcs_result_path,
            result_generator
        )

        # Unificazione e upload file CSV (batch metrics file)
        res.logger.info(f"[CRF][main][merge_handler] -> Saving {n_blobs} batch metrics files in '{gcs_metrics_path}'")
        merged_metrics = mrg.merge_csv_blobs(met_blobs)
        gcs_metrics_path = posixpath.join(res.gcs_result_dir, f"{dataset_name}_metrics.csv")
        gcs.upload_csv_data(
            bucket,
            gcs_metrics_path,
            merged_metrics,
            fieldnames=["batch_id", "n_alerts", "time_sec", "ram_mb"]
        )

    except Exception as e:
        res.logger.error(f"[CRF][main][merge_handler] -> Error ({type(e).__name__}): {str(e)}")
        raise