import json
import utils.vertexai_utils as vxc

from google.cloud import storage
from utils.logger_utils import logger


ASSET_BUCKET_NAME = "main-asset-storage"
CONFIG_FILENAME = "config.json"


class ResourceManager:
    def __init__(self):
        self._initialized = False
        self._logger = logger
        self._model = None
        self._gen_conf = None
        self._bucket = None
        self._n_batches = 3
        self._max_concurrent_requests = 8
        self._max_cache_age = 60 * 60 * 24 * 7
        self._gcs_dataset_dir = "datasets"
        self._gcs_cache_dir = "cache"
        self._gcs_result_dir = "results"
        self._gcs_batch_result_dir = "batch_results"
        # (dove possibile, impostare come valori di default quelli locali al server Fast API)
        self.initialize()

    def initialize(self):
        if self._initialized:
            return
        
        # Inizializzazione Vertex AI
        vxc.init()
        self._model = vxc.get_model()
        self._gen_conf = vxc.get_generation_config()
        
        # Connessione al bucket GCS
        self._bucket = storage.Client().bucket(ASSET_BUCKET_NAME)

        # Download variabili d'ambiente condivise su GCS
        conf = json.loads(self._bucket.blob(CONFIG_FILENAME).download_as_text())

        # Variabili d'ambiente condivise su GCS
        self._n_batches = conf.get("n_batches", self._n_batches)
        self._max_concurrent_requests = conf.get("max_concurrent_requests", self._max_concurrent_requests)
        self._max_cache_age = conf.get("max_cache_age", self._max_cache_age)
        self._gcs_dataset_dir = conf.get("gcs_dataset_dir", self._gcs_dataset_dir)
        self._gcs_cache_dir = conf.get("gcs_cache_dir", self._gcs_cache_dir)
        self._gcs_result_dir = conf.get("gcs_result_dir", self._gcs_result_dir)
        self._gcs_batch_result_dir = conf.get("gcs_batch_result_dir", self._gcs_batch_result_dir)

        self._initialized = True
        self._logger.info("[CRF][resource_manager][initialize] Initialization completed")


    @property
    def logger(self):
        return self._logger
    
    @property
    def model(self):
        return self._model

    @property
    def gen_conf(self):
        return self._gen_conf
    
    
    @property
    def bucket(self):
        return self._bucket
    
    @property   # D
    def n_batches(self):
        return self._n_batches
    
    @property
    def max_concurrent_requests(self):
        return self._max_concurrent_requests
    
    @property
    def max_cache_age(self):
        return self._max_cache_age

    @property   # D
    def gcs_dataset_dir(self):
        return self._gcs_dataset_dir
    
    @property
    def gcs_cache_dir(self):
        return self._gcs_cache_dir
    
    @property   # D
    def gcs_result_dir(self):
        return self._gcs_result_dir
    
    @property
    def gcs_batch_result_dir(self):
        return self._gcs_batch_result_dir


# Istanza singletone da far importare agli altri moduli
resource_manager = ResourceManager()