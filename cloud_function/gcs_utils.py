#NB: non usare la classe ResourceManager, o potrebbero verificarsi dei loop di import

import json

from google.cloud import storage
from logger_utils import logger


def download_config() -> dict:
    # Connessione al bucket
    blob = storage.Client().bucket("main-asset-storage").blob("config.json")

    if not blob.exists():
        logger.info(f"[CRF][gcs_utils][download_config] Configuration file not found")
        return {}

    config = json.loads(blob.download_as_text())
    return config
