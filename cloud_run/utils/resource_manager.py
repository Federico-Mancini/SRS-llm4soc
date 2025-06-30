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
        self._alerts_per_batch = 10
        self._max_concurrent_requests = 4
        self._max_cache_age = 60 * 60 * 24 * 7
        self._asset_bucket_name = "main-asset-storage"
        self._gcs_dataset_dir = "input_datasets"
        self._gcs_batch_dir = "input_batches"
        self._gcs_cache_dir = "cache"
        self._gcs_result_dir = "results"
        self._gcs_batch_result_dir = "batch_results"
        self._vms_config_filename = "config.json"
        self._runner_url = "https://llm4soc-runner-870222336278.europe-west1.run.app"
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
        self._alerts_per_batch = conf.get("alerts_per_batch", self._alerts_per_batch)
        self._max_concurrent_requests = conf.get("max_concurrent_requests", self._max_concurrent_requests)
        self._max_cache_age = conf.get("max_cache_age", self._max_cache_age)
        self._asset_bucket_name = conf.get("asset_bucket_name", self._asset_bucket_name)
        self._gcs_dataset_dir = conf.get("gcs_dataset_dir", self._gcs_dataset_dir)
        self._gcs_batch_dir = conf.get("gcs_batch_dir", self._gcs_batch_dir)
        self._gcs_cache_dir = conf.get("gcs_cache_dir", self._gcs_cache_dir)
        self._gcs_result_dir = conf.get("gcs_result_dir", self._gcs_result_dir)
        self._gcs_batch_result_dir = conf.get("gcs_batch_result_dir", self._gcs_batch_result_dir)
        self._vms_config_filename = conf.get("vms_config_filename", self._vms_config_filename)
        self._runner_url = conf.get("runner_url", self._runner_url)

        self._initialized = True
        self._logger.info("[CRR][resource_manager][initialize] Initialization completed")


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
    
    @property
    def alerts_per_batch(self):
        return self._alerts_per_batch
    
    @property
    def max_concurrent_requests(self):
        return self._max_concurrent_requests

    @property
    def max_cache_age(self):
        return self._max_cache_age
    
    @property
    def asset_bucket_name(self):
        return self._asset_bucket_name

    @property
    def gcs_dataset_dir(self):
        return self._gcs_dataset_dir
    
    @property
    def gcs_batch_dir(self):
        return self._gcs_batch_dir
    
    @property
    def gcs_cache_dir(self):
        return self._gcs_cache_dir

    @property
    def gcs_result_dir(self):
        return self._gcs_result_dir
    
    @property
    def gcs_batch_result_dir(self):
        return self._gcs_batch_result_dir
    
    @property
    def vms_config_filename(self):
        return self._vms_config_filename

    @property
    def runner_url(self):
        return self._runner_url


# Istanza singletone da far importare agli altri moduli
resource_manager = ResourceManager()