import os, json

from google.cloud import storage
from utils.logger_utils import logger


ASSET_BUCKET_NAME = "main-asset-storage"
CONFIG_FILENAME = "config.json"


class ResourceManager:
    def __init__(self):
        self._initialized = False
        self._logger = logger
        self._bucket = None
        self._alerts_per_batch = 100
        self._gcs_dataset_dir = "input_datasets"
        self._gcs_metrics_dir = "metrics"
        self._gcs_result_dir = "results"
        self._gcs_batch_metrics_dir = "batch_metrics"
        self._gcs_batch_result_dir = "batch_results"
        self._config_filename = "config.json"
        self._vms_config_path = "assets/config.json"
        self._vms_metrics_path = "assets/metrics.json"
        self._vms_result_path = "assets/result.json"
        self._project_id = ""
        self._location = ""
        self._batch_analysis_queue_name = "batch-analysis"
        self._runner_url = ""
        self._worker_url = ""
        self._vm_service_account_email = ""
        self.initialize()

    def initialize(self):
        if self._initialized:
            return
    
        # Connessione al bucket GCS
        self._bucket = storage.Client().bucket(ASSET_BUCKET_NAME)

        # Download variabili d'ambiente condivise su GCS
        conf = self.get_config()

        # Variabili d'ambiente condivise su GCS
        self._alerts_per_batch = conf.get("alerts_per_batch", self._alerts_per_batch)
        self._gcs_dataset_dir = conf.get("gcs_dataset_dir", self._gcs_dataset_dir)
        self._gcs_metrics_dir = conf.get("gcs_metrics_dir", self._gcs_metrics_dir)
        self._gcs_result_dir = conf.get("gcs_result_dir", self._gcs_result_dir)
        self._gcs_batch_metrics_dir = conf.get("gcs_batch_metrics_dir", self._gcs_batch_metrics_dir)
        self._gcs_batch_result_dir = conf.get("gcs_batch_result_dir", self._gcs_batch_result_dir)
        self._config_filename = conf.get("config_filename", self._config_filename)
        self._vms_config_path = conf.get("vms_config_path", self._vms_config_path)
        self._vms_metrics_path = conf.get("vms_metrics_path", self._vms_metrics_path)
        self._vms_result_path = conf.get("vms_result_path", self._vms_result_path)
        self._project_id = conf.get("project_id", self._project_id)
        self._location = conf.get("location", self._location)
        self._batch_analysis_queue_name = conf.get("batch_analysis_queue_name", self._batch_analysis_queue_name)
        self._runner_url = conf.get("runner_url", self._runner_url)
        self._worker_url = conf.get("worker_url", self._worker_url)
        self._vm_service_account_email = conf.get("vm_service_account_email", self._vm_service_account_email)

        self._initialized = True
        self._logger.info("[VMS][resource_manager][initialize] Initialization completed")

    def get_config(self) -> dict:
        try:
            return json.loads(self._bucket.blob(CONFIG_FILENAME).download_as_text())
        except Exception:
            self._logger.warning(f"[VMS][resource_manager][get_config] File {CONFIG_FILENAME} not found on GCS. Using local version as fallback")
            with open(os.path.join("assets", CONFIG_FILENAME), "r") as f:
                return json.load(f)


    @property
    def logger(self):
        return self._logger
    
    @property
    def bucket(self):
        return self._bucket
    
    @property
    def alerts_per_batch(self):
        return self._alerts_per_batch
    
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
    def config_filename(self):
        return self._config_filename
    
    @property
    def vms_config_path(self):
        return self._vms_config_path

    @property
    def vms_metrics_path(self):
        return self._vms_metrics_path

    @property
    def vms_result_path(self):
        return self._vms_result_path
    
    @property
    def project_id(self):
        return self._project_id
    
    @property
    def location(self):
        return self._location

    @property
    def batch_analysis_queue_name(self):
        return self._batch_analysis_queue_name
    
    @property
    def runner_url(self):
        return self._runner_url

    @property
    def worker_url(self):
        return self._worker_url

    @property
    def vm_service_account_email(self):
        return self._vm_service_account_email


# Istanza singletone da far importare agli altri moduli
resource_manager = ResourceManager()