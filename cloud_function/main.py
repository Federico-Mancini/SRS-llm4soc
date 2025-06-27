import json
import gcs_utils as gcs

from google.cloud import storage


initialized = False
N_BATCHES = 3
RESULT_FILENAME = ""
GCS_RESULT_DIR = ""


# provvisorio
def merge_handler():
    print("I'm merge handler!!!!")

# # -- Funzioni -------------------------------------------------

# # Inizializzazione var. d'ambiente
# def initialize():
#     global initialized, N_BATCHES, RESULT_FILENAME, GCS_RESULT_DIR

#     if not initialized:
#         # Estrazione di variabili d'ambiente (condivise su GCS)
#         conf = gcs.download_config()

#         if not conf or not all(k in conf for k in ("n_batches", "result_filename", "gcs_result_dir")):
#             raise ValueError("[main|initialize] Configurazione non valida. Impossibile completare il merge")

#         N_BATCHES = conf["n_batches"]
#         RESULT_FILENAME = conf["result_filename"]
#         GCS_RESULT_DIR = conf["gcs_result_dir"]

#         initialized = True


# # Merge single result files in one final 'result.json' (eseguita come Cloud Function al trigger di GCS, cioè ogni volta che un file 'result' viene creato)
# def merge_handler(event, context):
#     initialize()

#     # Connessione al bucket
#     bucket_name = event['bucket']                   # estrazione nome bucket da chi ha generato l'evento trigger
#     bucket = storage.Client().bucket(bucket_name)
#     blobs = list(bucket.list_blobs(prefix=f"{GCS_RESULT_DIR}/result-"))

#     # Controllo presenza di tutti i file attesi
#     if len(blobs) < N_BATCHES:
#         print(f"Solo {len(blobs)}/{N_BATCHES} file 'result' presenti. In attesa di altri...")
#         return

#     # Unione dei file 'result'
#     merged = []
#     for blob in blobs:
#         try:
#             data = json.loads(blob.download_as_text())
#             if isinstance(data, list):
#                 merged.extend(data)
#         except Exception as e:
#             print(f"Impossibile leggere {blob.name}: {e}")

#     merged_blob = bucket.blob(RESULT_FILENAME)                      # creazione riferimento a nuovo file nel bucket
#     merged_blob.upload_from_string(json.dumps(merged, indent=2))    # caricamento dati effettivo

#     print(f"{len(merged)} alert salvati in {RESULT_FILENAME}")

#     # Rimozione singoli file 'result' (non più necessari)
#     for blob in blobs:
#         blob.delete()
