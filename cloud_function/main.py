import os, json
import gcs_utils as gcs

from google.cloud import storage


# Estrazione di variabili d'ambiente (condivise su GCS)
conf = gcs.download_config()
N_BATCHES, RESULT_FILENAME, GCS_RESULT_DIR = (
    conf.n_batches,
    conf.result_filename,
    conf.gcs_result_dir
)


# -- Funzioni -------------------------------------------------

# Merge single result files in one final 'result.json' (eseguita come Cloud Function al trigger di GCS, cioè ogni volta che un file 'result' viene creato)
def merge_handler(event, context):
    n_batches = int(os.getenv("N_BATCHES", N_BATCHES))

    # Connessione al bucket
    bucket_name = event['bucket']                   # estrazione nome bucket da chi ha generato l'evento trigger
    bucket = storage.Client().bucket(bucket_name)
    blobs = list(bucket.list_blobs(prefix=f"{GCS_RESULT_DIR}/result-"))

    # Controllo presenza di tutti i file attesi
    if len(blobs) < n_batches:
        print(f"Solo {len(blobs)}/{n_batches} file 'result' presenti. In attesa di altri...")
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

    merged_blob = bucket.blob(RESULT_FILENAME)                      # creazione riferimento a nuovo file nel bucket
    merged_blob.upload_from_string(json.dumps(merged, indent=2))    # caricamento dati effettivo

    print(f"{len(merged)} alert salvati in {RESULT_FILENAME}")

    # Rimozione singoli file 'result' (non più necessari)
    for blob in blobs:
        blob.delete()

'''
Comando per il deploy di "merge_handler" come Cloud Function da invocare alla creazione di un file nella cartella "results" del bucket:
(Nota: da eseguire su host - copia e incolla funzione in un nuovo python, dedicato solo a questa funzione. Poi esegui il comando nella cartella contenente il file)

gcloud functions deploy merge_handler --runtime python311 --trigger-resource main-asset-storage --trigger-event google.storage.object.finalize --entry-point merge_handler --region europe-west1
'''