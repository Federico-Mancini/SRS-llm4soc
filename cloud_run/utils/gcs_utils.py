import json
from google.cloud import storage


def download_config() -> dict:
    # Connessione al bucket
    blob = storage.Client().bucket("main-asset-storage").blob("config.json")

    if not blob.exists():
        print(f"Configurazione non trovata: 'config.json'")
        return {}

    config = json.loads(blob.download_as_text())
    return config
