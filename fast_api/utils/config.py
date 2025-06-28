import os, multiprocessing


N_BATCHES = 3    # numero di file batch in cui suddividere il dataset di alert originale


# File system constants
ASSET_DIR = "assets"

CACHE_FILENAME = "cache.json"
RESULT_FILENAME = "result.json"
ALERTS_FILENAME = "alert_dataset.csv"

CACHE_PATH = os.path.join(ASSET_DIR, CACHE_FILENAME)
RESULT_PATH = os.path.join(ASSET_DIR, RESULT_FILENAME)
ALERTS_PATH = os.path.join(ASSET_DIR, ALERTS_FILENAME)


# # FastAPI endpoint
# UPLOAD_ENDPOINT = "/upload"
# RESULTS_ENDPOINT = "/results"
# ANALYZE_ENDPOINT = "/analyze"
# CHAT_ENDPOINT = "/chat"
# ANALYZE_ALL_ENDPOINT = "/analyze_all"


# GCS
ASSET_BUCKET_NAME = "main-asset-storage"
GCS_BATCH_DIR = "input_batches" # cartella per i file con partizioni di batch originale
GCS_CACHE_DIR = "cache"         # cartella per i file di cache dedicati ai singoli alert
GCS_RESULT_DIR = "results"      # cartella per i file con i risultati


# Cache logic
cpu_count = multiprocessing.cpu_count()         # num CPU disponibili
MAX_CONCURRENT_REQUESTS = max(5, cpu_count-1)   # max richieste/thread simultanei (minimo default: 5 - testare eventuali soglie maggiori)
MAX_CACHE_AGE = 60 * 60 * 24 * 7                # max periodo di permanenza di entry in cache (7 giorni)


# Vertex AI
PROJECT_ID = os.environ.get("GOOGLE_CLOUD_PROJECT") or "gruppo-4-456912"
LOCATION = os.environ.get("GOOGLE_CLOUD_REGION") or "europe-west1"
MODEL_NAME = "gemini-2.0-flash-001"


# Cluod Run
CLOUD_RUN_URL = "https://llm4soc-runner-870222336278.europe-west1.run.app"