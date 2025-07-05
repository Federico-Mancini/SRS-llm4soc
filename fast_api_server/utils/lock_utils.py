from utils.resource_manager import resource_manager as res


# F01 - Rimozione del flag di controllo che sospende l'attività del merge handler
def release_merge_lock():
    flag_path = f"{res.gcs_flag_dir}/{res.merge_lock_flag_filename}"
    blob = res.bucket.blob(flag_path)

    try:    
        if blob.exists():
            blob.delete()
    except Exception as e:
        res.logger.error(f"[lock|F01]\t\t-> Failed to delete merge lock flag ({type(e).__name__}): {str(e)}")
        raise
