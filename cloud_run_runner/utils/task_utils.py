import json

from google.cloud import tasks_v2
from utils.resource_manager import resource_manager as res


# Invio richieste multiple di analisi batch al worker
def enqueue_slicing_tasks(dataset_metadata: json):
    client = tasks_v2.CloudTasksClient()
    parent = client.queue_path(res.project_id, res.location, res.analysis_batch_queue_name)

    metadata = json.loads(dataset_metadata)

    # Controllo ed estrazione campi
    required_fields = ["num_rows", "num_batches", "batch_size", "dataset_path"]
    missing = [field for field in required_fields if field not in metadata or metadata[field] is None]
    
    if missing:
        return {"error": f"Missing required fields: {', '.join(missing)}"}

    num_rows, num_batches, batch_size, dataset_path = (metadata[field] for field in required_fields)

    # Invio richieste, una per batch
    for i in range(num_batches):
        payload = {
            "batch_id": i,
            "start_row": i * batch_size,
            "end_row": min((i + 1) * batch_size, num_rows),
            "batch_size": batch_size,
            "dataset_path": dataset_path
        }

        task = {
            "http_request": {
                "http_method": tasks_v2.HttpMethod.POST,
                "url": res.worker_url,
                "headers": {"Content-Type": "application/json"},
                "body": json.dumps(payload).encode()
            }
        }

        response = client.create_task(parent=parent, task=task)
        res.logger.debug(f"[CRR][task_utils][enqueue_slicing_tasks] -> Created task for batch {i}: {response.name}")
    
    res.logger.info(f"[CRR][task_utils][enqueue_slicing_tasks] -> {num_batches} tasks created")
