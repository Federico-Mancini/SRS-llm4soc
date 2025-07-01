import json

from google.cloud import storage
from utils.logger_utils import logger


ASSET_BUCKET_NAME = "main-asset-storage"
CONFIG_FILENAME = "config.json"


class ResourceManager:
    def __init__(self):
        self._initialized = False
        self._logger = logger
        self._bucket = None
        self._n_batches = 3
        self._max_concurrent_requests = 8
        self._gcs_dataset_dir = "input_datasets"
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
        self._n_batches = conf.get("n_batches", self._n_batches)
        self._max_concurrent_requests = conf.get("max_concurrent_requests", self._max_concurrent_requests)
        self._gcs_dataset_dir = conf.get("gcs_dataset_dir", self._gcs_dataset_dir)
        self._gcs_result_dir = conf.get("gcs_result_dir", self._gcs_result_dir)
        self._gcs_batch_result_dir = conf.get("gcs_batch_result_dir", self._gcs_batch_result_dir)

        self._initialized = True
        self._logger.info("[CRF][resource_manager][initialize] Initialization completed")


    @property
    def logger(self):
        return self._logger
    
    @property
    def bucket(self):
        return self._bucket
    
    @property
    def n_batches(self):
        return self._n_batches
    
    @property
    def max_concurrent_requests(self):
        return self._max_concurrent_requests

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