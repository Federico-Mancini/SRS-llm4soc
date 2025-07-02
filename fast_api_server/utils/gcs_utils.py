import os, posixpath

from utils.resource_manager import resource_manager as res


# Costruzione path remoto (usato in VMS per file 'result', 'metrics' e 'metadata')
def get_blob_path(folder: str, dataset_filename: str, suffix: str, file_format: str) -> str:
    dataset_name = os.path.splitext(dataset_filename)[0]
    return posixpath.join(folder, f"{dataset_name}_{suffix}.{file_format}")


# Svuotamento directory
def empty_dir(gcs_dir: str):
    blobs = res.bucket.list_blobs(prefix=f"{gcs_dir}/")

    count = 0
    for blob in blobs:
        blob.delete()
        count += 1
    
    res.logger.info(f"[VMS][gcs_utils][empty_gcs_dir] {count} files deleted from '{gcs_dir}/'")
