import json

from google.cloud import tasks_v2
from utils.resource_manager import resource_manager as res


# Invio richieste multiple di analisi batch al worker
def enqueue_tasks(metadata: json):
    client = tasks_v2.CloudTasksClient()
    parent = client.queue_path(res.project_id, res.location, res.analysis_batch_queue_name)

    # Controllo ed estrazione campi
    required_fields = ["num_rows", "num_batches", "batch_size", "dataset_name", "dataset_path"]
    missing = [field for field in required_fields if field not in metadata or metadata[field] is None]
    
    if missing:
        return {"error": f"Missing required fields: {', '.join(missing)}"}

    num_rows, num_batches, batch_size, dataset_name, dataset_path = (metadata[field] for field in required_fields)

    # Invio richieste, una per batch
    for i in range(num_batches):
        payload = {
            "batch_id": i,
            "start_row": i * batch_size,
            "end_row": min((i + 1) * batch_size, num_rows),
            "dataset_name": dataset_name,
            "dataset_path": dataset_path
        }

        task = {
            "http_request": {
                "http_method": tasks_v2.HttpMethod.POST,
                "url": f"{res.worker_url}/run-batch",
                "headers": {"Content-Type": "application/json"},
                "body": json.dumps(payload).encode(),
                "oidc_token": {
                    "service_account_email": res.vm_service_account_email
                }
            }
        }

        response = client.create_task(parent=parent, task=task)
        res.logger.debug(f"[VMS][task_utils][enqueue_tasks] -> Created task for batch {i}: {response.name}")
    
    res.logger.info(f"[VMS][task_utils][enqueue_tasks] -> {num_batches} tasks created")
