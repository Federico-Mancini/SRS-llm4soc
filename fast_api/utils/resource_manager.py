import json

from google.cloud import storage
from logger_utils import logger


ASSET_BUCKET_NAME = "main-asset-storage"
CONFIG_FILENAME = "config.json"


class ResourceManager:
    def __init__(self):
        self._initialized = False
        self._logger = logger
        self._bucket = None
        self._asset_bucket_name = "main-asset-storage"
        self._gcs_dataset_dir = "input_datasets"
        self._gcs_result_dir = "results"
        self._vms_config_filename = "config.json"
        self._vms_result_path = "assets/result.json"
        self._runner_url = ""   # per sicurezza, valore di default vuoto

    def initialize(self):
        if self._initialized:
            return
    
        # Connessione al bucket GCS
        self._bucket = storage.Client().bucket(ASSET_BUCKET_NAME)

        # Download variabili d'ambiente condivise su GCS
        conf = json.loads(self._bucket.blob(CONFIG_FILENAME).download_as_text())

        # Variabili d'ambiente condivise su GCS
        self._asset_bucket_name = conf.get("asset_bucket_name", self._asset_bucket_name)
        self._gcs_dataset_dir = conf.get("gcs_dataset_dir", self._gcs_dataset_dir)
        self._gcs_result_dir = conf.get("gcs_result_dir", self._gcs_result_dir)
        self._vms_config_filename = conf.get("vms_config_filename", self._vms_config_filename)
        self._vms_result_path = conf.get("vms_result_path", self._vms_result_path)
        self._runner_url = conf.get("runner_url", self._runner_url)

        self._initialized = True
        self._logger.info("[VMS][resource_manager][initialize] Initialization completed")


    @property
    def logger(self):
        self.initialize()
        return self._logger
    
    @property
    def bucket(self):
        self.initialize()
        return self._bucket
    
    @property
    def asset_bucket_name(self):
        self.initialize()
        return self._asset_bucket_name
    
    @property
    def gcs_dataset_dir(self):
        self.initialize()
        return self._gcs_dataset_dir
    
    @property
    def gcs_result_dir(self):
        self.initialize()
        return self._gcs_result_dir
    
    @property
    def vms_config_filename(self):
        self.initialize()
        return self._vms_config_filename
    
    @property
    def vms_result_path(self):
        self.initialize()
        return self._vms_result_path
    
    @property
    def runner_url(self):
        self.initialize()
        return self._runner_url
