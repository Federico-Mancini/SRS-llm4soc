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
        self._max_concurrent_requests = 16
        self._max_cache_age = 60 * 60 * 24 * 7
        self._gcs_cache_dir = "cache"
        self._gcs_metrics_dir = "metrics"
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
        
        # Connessione ai bucket GCS
        self._bucket = storage.Client().bucket(ASSET_BUCKET_NAME)

        # Download variabili d'ambiente condivise su GCS
        conf = json.loads(self._bucket.blob(CONFIG_FILENAME).download_as_text())

        # Variabili d'ambiente condivise su GCS
        self._max_concurrent_requests = conf.get("max_concurrent_requests", self._max_concurrent_requests)
        self._max_cache_age = conf.get("max_cache_age", self._max_cache_age)
        self._gcs_cache_dir = conf.get("gcs_cache_dir", self._gcs_cache_dir)
        self._gcs_metrics_dir = conf.get("gcs_metrics_dir", self._gcs_metrics_dir)
        self._gcs_batch_result_dir = conf.get("gcs_batch_result_dir", self._gcs_batch_result_dir)

        # Warm-up modello Gemini (risolve il problema del Cold Start o del caricamento on-demand del modello AI)
        try:
            self._model.generate_content("ping", generation_config=self._gen_conf)
            self._logger.info("[CRW][resource_manager][initialize] Warm-up request sent to Gemini")
        except Exception as e:
            self._logger.warning(f"[CRW][resource_manager][initialize] Warm-up failed ({type(e).__name__}): {str(e)}")

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
    
    @property
    def max_concurrent_requests(self):
        return self._max_concurrent_requests
    
    @property
    def max_cache_age(self):
        return self._max_cache_age
    
    @property
    def gcs_cache_dir(self):
        return self._gcs_cache_dir
    
    @property
    def gcs_metrics_dir(self):
        return self._gcs_metrics_dir
    
    @property
    def gcs_batch_result_dir(self):
        return self._gcs_batch_result_dir


# Istanza singletone da far importare agli altri moduli
resource_manager = ResourceManager()