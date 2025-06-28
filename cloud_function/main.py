import json

from google.cloud import storage
from resource_manager import ResourceManager


res = ResourceManager()


# Merge single result files in one final 'result.json' (eseguita come Cloud Function al trigger di GCS, cioè ogni volta che un file 'result' viene creato)
def merge_handler(event, context):
    res.initialize()

    # Connessione al bucket
    bucket_name = event['bucket']                   # estrazione nome bucket da chi ha generato l'evento trigger
    bucket = storage.Client().bucket(bucket_name)
    blobs = list(bucket.list_blobs(prefix=f"{res.gcs_result_dir}/result-"))

    # Controllo presenza di tutti i file attesi
    if len(blobs) < res.n_batches:
        print(f"Solo {len(blobs)}/{res.n_batches} file 'result' presenti. In attesa di altri...")
        return

    # Unione dei file 'result'
    merged = []
    for blob in blobs:
        try:
            data = json.loads(blob.download_as_text())
            if isinstance(data, list):
                merged.extend(data)
        except Exception as e:
            print(f"Impossibile leggere {blob.name}: {e}")

    merged_blob = bucket.blob(res.result_filename)                  # creazione riferimento a nuovo file nel bucket
    merged_blob.upload_from_string(json.dumps(merged, indent=2))    # caricamento dati effettivo

    print(f"{len(merged)} alert salvati in {res.result_filename}")

    # Rimozione singoli file 'result' (non più necessari)
    for blob in blobs:
        blob.delete()
