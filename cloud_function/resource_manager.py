import json

from google.cloud import storage
from logger_utils import logger


ASSET_BUCKET_NAME = "main-asset-storage"
CONFIG_FILENAME = "config.json"


class ResourceManager:
    def __init__(self):
        self._initialized = False
        self._logger = logger
        self._gcs_dataset_dir = "datasets"
        self._gcs_result_dir = "results"
        self._gcs_batch_result_dir = "batch_results"
        # (dove possibile, impostare come valori di default quelli locali al server Fast API)
        self.initialize()

    def initialize(self):
        if self._initialized:
            return
        
        # Connessione al bucket GCS
        self._bucket = storage.Client().bucket(ASSET_BUCKET_NAME)

        # Download variabili d'ambiente condivise su GCS
        conf = json.loads(self._bucket.blob(CONFIG_FILENAME).download_as_text())

        # Variabili d'ambiente condivise su GCS
        self._gcs_dataset_dir = conf.get("gcs_dataset_dir", self._gcs_dataset_dir)
        self._gcs_result_dir = conf.get("gcs_result_dir", self._gcs_result_dir)
        self._gcs_batch_result_dir = conf.get("gcs_batch_result_dir", self._gcs_batch_result_dir)

        self._initialized = True
        self._logger.info("[CRF][resource_manager][initialize] -> Initialization completed")


    @property
    def logger(self):
        return self._logger

    @property
    def gcs_dataset_dir(self):
        return self._gcs_dataset_dir

    @property
    def gcs_result_dir(self):
        return self._gcs_result_dir
    
    @property
    def gcs_batch_result_dir(self):
        return self._gcs_batch_result_dir


# Istanza singletone da far importare agli altri moduli
resource_manager = ResourceManager()