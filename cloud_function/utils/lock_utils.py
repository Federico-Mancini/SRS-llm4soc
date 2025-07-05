from google.cloud import storage
from utils.resource_manager import resource_manager as res


# F01 - Acquisizione lock (creazione flag)
def acquire_lock(bucket: storage.Bucket) -> bool:
    lock_path = f"{res.gcs_flag_dir}/{res.merge_lock_flag_filename}"
    try:
        bucket.blob(lock_path).upload_from_string("lock", if_generation_match=0)  # l'attributo 'if_generation_match' fa sì che so tenti la creazione solo se il file non esiste già
        return True
    except Exception:   # eccezione lanciata in presenza del flag
        return False


# F02 - Rilascio lock (eliminazione flag)
# def release_lock(bucket: storage.Bucket):
#     lock_path = f"{res.gcs_flag_dir}/{res.merge_lock_flag_filename}"
#     try:
#         bucket.blob(lock_path).delete()
#     except Exception as e:
#         res.logger.error(f"[GCS][release_lock] -> Failed to release lock ({type(e).__name__}): {str(e)}")

# NB: l'eliminazione del flag è delegata all'endpoint '/analyze-dataset' del server FastAPI