import json

from google.cloud import storage
from utils.logger_utils import logger


ASSET_BUCKET_NAME = "main-asset-storage"
CONFIG_FILENAME = "config.json"


class ResourceManager:
    def __init__(self):
        self._initialized = False
        self._logger = logger
        self._gcs_flag_dir = "control_flags"
        self._gcs_dataset_dir = "datasets"
        self._gcs_metrics_dir = "metrics"
        self._gcs_result_dir = "results"
        self._gcs_batch_metrics_dir = "batch_metrics"
        self._gcs_batch_result_dir = "batch_results"
        self._merge_stop_flag_filename = "merge_stop.flag"
        # (dove possibile, impostare come valori di default quelli locali al server Fast API)
        self.initialize()

    def initialize(self):
        if self._initialized:
            return
        
        # Connessione ai bucket GCS
        self._bucket = storage.Client().bucket(ASSET_BUCKET_NAME)   # NB: non condividere questa variabile in quanto ricavata dall'evento trigger

        # Download variabili d'ambiente condivise su GCS
        conf = json.loads(self._bucket.blob(CONFIG_FILENAME).download_as_text())

        # Variabili d'ambiente condivise su GCS
        self._gcs_flag_dir = conf.get("gcs_flag_dir", self._gcs_flag_dir)
        self._gcs_dataset_dir = conf.get("gcs_dataset_dir", self._gcs_dataset_dir)
        self._gcs_metrics_dir = conf.get("gcs_metrics_dir", self._gcs_metrics_dir)
        self._gcs_result_dir = conf.get("gcs_result_dir", self._gcs_result_dir)
        self._gcs_batch_metrics_dir = conf.get("gcs_batch_metrics_dir", self._gcs_batch_metrics_dir)
        self._gcs_batch_result_dir = conf.get("gcs_batch_result_dir", self._gcs_batch_result_dir)
        self._merge_stop_flag_filename = conf.get("merge_stop_flag_filename", self._merge_stop_flag_filename)

        self._initialized = True
        self._logger.info("[RM|F02]\t-> Resource manager initialized")


    @property
    def logger(self):
        return self._logger

    @property
    def gcs_flag_dir(self):
        return self._gcs_flag_dir
    
    @property
    def gcs_dataset_dir(self):
        return self._gcs_dataset_dir
    
    @property
    def gcs_metrics_dir(self):
        return self._gcs_metrics_dir

    @property
    def gcs_result_dir(self):
        return self._gcs_result_dir
    
    @property
    def gcs_batch_metrics_dir(self):
        return self._gcs_batch_metrics_dir

    @property
    def gcs_batch_result_dir(self):
        return self._gcs_batch_result_dir

    @property
    def merge_stop_flag_filename(self):
        return self._merge_stop_flag_filename


# Istanza singletone da far importare agli altri moduli
resource_manager = ResourceManager()