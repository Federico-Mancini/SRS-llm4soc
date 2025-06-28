import utils.vertexai_utils as vxc
import utils.gcs_utils as gcs

from google.cloud import storage


class ResourceManager:
    def __init__(self):
        self.initialized = False
        self.model = None
        self.gen_conf = None
        self.bucket = None
        self.gcs_cache_dir = "cache"
        self.gcs_result_dir = "results"
        self.max_concurrent_requests = 5
        self.max_cache_age = 60 * 60 * 24 * 7
        # (dove possibile, impostare come valori di default quelli locali al server Fast API)

    def initialize(self):
        if not self.initialized:
            vxc.init()
            self.model = vxc.get_model()
            self.gen_conf = vxc.get_generation_config()

            conf = gcs.download_config()
            self.gcs_cache_dir = conf["gcs_cache_dir"]
            self.gcs_result_dir = conf["gcs_result_dir"]
            self.max_concurrent_requests = conf["max_concurrent_requests"]
            self.max_cache_age = conf["max_cache_age"]

            self.bucket = storage.Client().bucket(conf["asset_bucket_name"])
            self.initialized = True
