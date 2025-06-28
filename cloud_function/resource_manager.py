import gcs_utils as gcs


class ResourceManager:
    def __init__(self):
        self.initialized = False
        self.n_batches = 3
        self.result_filename = "result.json"
        self.gcs_result_dir = "results"
        # (dove possibile, impostare come valori di default quelli locali al server Fast API)

    def initialize(self):
        if not self.initialized:
            conf = gcs.download_config()
            self.n_batches = conf["n_batches"]
            self.result_filename = conf["result_filename"]
            self.gcs_result_dir = conf["gcs_result_dir"]
            
            self.initialized = True
