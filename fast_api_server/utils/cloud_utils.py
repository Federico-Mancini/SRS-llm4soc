# Cloud Utils: modulo per la gestione delle comunicazioni server-worker (VSM-CRW)

import json, httpx

from google.cloud import tasks_v2
from utils.resource_manager import resource_manager as res
from utils.auth_utils import get_auth_header


# F01 - Gestore chiamate al worker in esecuzione su Cloud Run
async def call_worker(method: str, url: str, json: dict = None, timeout: float = 30.0) -> dict:
    headers = get_auth_header(url)

    try:
        async with httpx.AsyncClient(timeout=timeout) as client:
            if method.upper() == "GET":
                response = await client.get(url, headers=headers)
            elif method.upper() == "POST":
                response = await client.post(url, headers=headers, json=json)
            else:
                raise ValueError("[cloud|F01]\t\t-> Metodo HTTP non supportato")
            
            response.raise_for_status()
            return response.json()

    except httpx.RequestError as e:
        res.logger.error(f"[cloud|F01]\t\t-> Connection error ({type(e).__name__}): {str(e)}")
        raise

    except httpx.HTTPStatusError as e:
        res.logger.error(f"[cloud|F01]\t\t-> Invalid HTTP response ({type(e).__name__}): {str(e)}")
        raise

    except Exception as e:
        res.logger.error(f"[cloud|F01]\t\t-> {type(e).__name__}: {str(e)}")
        raise


# F02 - Invio richieste multiple per l'analisi degli alert che compongono il batch
def enqueue_batch_analysis_tasks(metadata: json):
    client = tasks_v2.CloudTasksClient()
    parent = client.queue_path(res.project_id, res.location, res.batch_analysis_queue_name)

    # Controllo ed estrazione campi
    required_fields = ["num_rows", "num_batches", "batch_size", "dataset_name", "dataset_path"]
    missing = [field for field in required_fields if field not in metadata or metadata[field] is None]
    
    if missing:
        msg = f"Missing required fields: {', '.join(missing)}"
        res.logger.warning(msg)
        raise httpx.HTTPException(status_code=500, detail=msg)

    num_rows, num_batches, batch_size, dataset_name, dataset_path = (metadata[field] for field in required_fields)

    # Invio richieste, una per batch
    for i in range(num_batches):
        payload = {
            "batch_id": i,
            "start_row": i * batch_size,
            "end_row": min((i + 1) * batch_size, num_rows),
            "batch_size": batch_size,
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

        client.create_task(parent=parent, task=task)

    res.logger.info(f"[cloud|F02]\t\t-> {num_batches} tasks created")
