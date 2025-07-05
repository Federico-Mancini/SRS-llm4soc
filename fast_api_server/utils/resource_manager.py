import os, json

from google.cloud import storage
from utils.logger_utils import logger


ASSET_BUCKET_NAME = "main-asset-storage"
CONFIG_FILENAME = "config.json"


class ResourceManager:
    # F01 - Costruttore
    def __init__(self):
        self._initialized = False
        self._logger = logger
        self._bucket = None

        self._batch_size = 100
        self._max_concurrent_requests = 16
        self._not_available = "N/A"

        self._gcs_flag_dir = "control_flags"
        self._gcs_dataset_dir = "input_datasets"
        self._gcs_metrics_dir = "metrics"
        self._gcs_result_dir = "results"
        self._gcs_batch_metrics_dir = "batch_metrics"
        self._gcs_batch_result_dir = "batch_results"

        self._merge_stop_flag_filename = "merge_stop.flag"
        self._config_filename = "config.json"
        self._ml_dataset_filename = "training_reg_data.cvs"

        self._vms_config_path = "assets/config.json"
        self._vms_config_backup_path = "assets/config_backup.json"
        self._vms_benchmark_context_path = "assets/benchmark_context.json"
        self._vms_benchmark_stop_flag = "assets/benchmark_stop.flag"
        self._vms_metrics_path = "assets/metrics.json"
        self._vms_result_path = "assets/result.json"
        self._vms_ml_dataset_path = "assets/training_reg_data.csv"

        self._project_id = ""
        self._location = ""

        self._batch_analysis_queue_name = "batch-analysis"

        self._runner_url = ""
        self._worker_url = ""

        self._vm_service_account_email = ""
        self.initialize()

    # F02 - Inizializzazione
    def initialize(self):
        if self._initialized:
            return
    
        # Connessione al bucket GCS
        self._bucket = storage.Client().bucket(ASSET_BUCKET_NAME)

        # Download variabili d'ambiente condivise su GCS
        conf = self.get_config()

        # Variabili d'ambiente condivise su GCS
        self._batch_size = conf.get("batch_size", self._batch_size)
        self._max_concurrent_requests = conf.get("max_concurrent_requests", self._max_concurrent_requests)
        self._not_available = conf.get("not_available", self._not_available)

        self._gcs_flag_dir = conf.get("gcs_flag_dir", self._gcs_flag_dir)
        self._gcs_dataset_dir = conf.get("gcs_dataset_dir", self._gcs_dataset_dir)
        self._gcs_metrics_dir = conf.get("gcs_metrics_dir", self._gcs_metrics_dir)
        self._gcs_result_dir = conf.get("gcs_result_dir", self._gcs_result_dir)
        self._gcs_batch_metrics_dir = conf.get("gcs_batch_metrics_dir", self._gcs_batch_metrics_dir)
        self._gcs_batch_result_dir = conf.get("gcs_batch_result_dir", self._gcs_batch_result_dir)
        
        self._merge_stop_flag_filename = conf.get("merge_stop_flag_filename", self._merge_stop_flag_filename)
        self._config_filename = conf.get("config_filename", self._config_filename)
        self._ml_dataset_filename = conf.get("ml_dataset_filename", self._ml_dataset_filename)
        
        self._vms_config_path = conf.get("vms_config_path", self._vms_config_path)
        self._vms_config_backup_path = conf.get("vms_config_backup_path", self._vms_config_backup_path)
        self._vms_benchmark_context_path = conf.get("vms_benchmark_context_path", self._vms_benchmark_context_path)
        self._vms_benchmark_stop_flag = conf.get("vms_benchmark_stop_flag", self._vms_benchmark_stop_flag)
        self._vms_metrics_path = conf.get("vms_metrics_path", self._vms_metrics_path)
        self._vms_result_path = conf.get("vms_result_path", self._vms_result_path)
        self._vms_ml_dataset_path = conf.get("vms_ml_dataset_path", self._vms_ml_dataset_path)
        
        self._project_id = conf.get("project_id", self._project_id)
        self._location = conf.get("location", self._location)
        
        self._batch_analysis_queue_name = conf.get("batch_analysis_queue_name", self._batch_analysis_queue_name)
        
        self._runner_url = conf.get("runner_url", self._runner_url)
        self._worker_url = conf.get("worker_url", self._worker_url)
        
        self._vm_service_account_email = conf.get("vm_service_account_email", self._vm_service_account_email)

        self._initialized = True
        self._logger.info("[RM|F02]\t\t-> Resource manager initialized")

    # F03 - Lettura file di configurazione
    def get_config(self) -> dict:
        try:
            return json.loads(self._bucket.blob(CONFIG_FILENAME).download_as_text())
        except Exception:
            self._logger.warning(f"[RM|F03]\t\t-> File {CONFIG_FILENAME} not found on GCS. Using local version as fallback")
            with open(os.path.join("assets", CONFIG_FILENAME), "r") as f:
                return json.load(f)
    
    # F04 - Aggiornamento dei valori assegnati alle variabili private
    def reload_config(self):
        conf = json.loads(self._bucket.blob(CONFIG_FILENAME).download_as_text())
        self._batch_size = conf.get("batch_size", self._batch_size)
        self._max_concurrent_requests = conf.get("max_concurrent_requests", self._max_concurrent_requests)


    @property
    def logger(self):
        return self._logger
    
    @property
    def bucket(self):
        return self._bucket
    
    @property
    def batch_size(self):
        return self._batch_size
    
    @property
    def max_concurrent_requests(self):
        return self._max_concurrent_requests
    
    @property
    def not_available(self):
        return self._not_available
    
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
    def config_filename(self):
        return self._config_filename
    
    @property
    def ml_dataset_filename(self):
        return self._ml_dataset_filename
    
    @property
    def vms_config_path(self):
        return self._vms_config_path

    @property
    def vms_config_backup_path(self):
        return self._vms_config_backup_path

    @property
    def vms_benchmark_context_path(self):
        return self._vms_benchmark_context_path
    
    @property
    def vms_benchmark_stop_flag(self):
        return self._vms_benchmark_stop_flag

    @property
    def vms_metrics_path(self):
        return self._vms_metrics_path

    @property
    def vms_result_path(self):
        return self._vms_result_path
    
    @property
    def vms_ml_dataset_path(self):
        return self._vms_ml_dataset_path
    
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

    @property
    def merge_stop_flag_filename(self):
        return self._merge_stop_flag_filename


# Istanza singletone da far importare agli altri moduli
resource_manager = ResourceManager()