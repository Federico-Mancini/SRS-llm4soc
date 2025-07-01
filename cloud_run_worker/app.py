# CRW: Cloud Run Worker

import io, json, posixpath
import pandas as pd

from fastapi import FastAPI, HTTPException, Request
from utils.resource_manager import resource_manager as res
from analyze_data import analyze_batch_sync, analyze_batch_sync_with_cache, analyze_batch_async, analyze_batch_async_with_cache


app = FastAPI()


# Endpoint dedicato alla ricezione di richieste anomale dirette alla root del worker
@app.api_route("/", methods=["GET", "POST"])
async def block_root():
    raise HTTPException(status_code=404, detail="Invalid endpoint")


# Ricezione richieste d'analisi del batch i-esimo
# Operazioni: estrazione batch dal dataset remoto -> classificazione alert -> creazione file result temporaneo
@app.post("/run-batch")
async def run_batch(request: Request):
    body = await request.json()

    # Controllo ed estrazione campi
    required_fields = ["batch_id", "start_row", "end_row", "dataset_name", "dataset_path"]
    missing = [field for field in required_fields if field not in body or body[field] is None]
    
    if missing:
        return {"error": f"Missing required fields: {', '.join(missing)}"}

    batch_id, start_row, end_row, dataset_name, dataset_path = (body[field] for field in required_fields)

    # Connessione al bucket
    blob = res.bucket.blob(dataset_path)
    data = blob.download_as_text()

    # Carica e suddividi dataset
    df = pd.read_json(io.StringIO(data), lines=True)
    batch_df = df.iloc[start_row:end_row]

    # Classificazione alert del batch
    #batch_result_list = analyze_batch_sync(batch_df, batch_id)
    batch_result_list = await analyze_batch_async_with_cache(batch_df, batch_id)
    #batch_result_list = await analyze_batch_async(batch_df, batch_id)   # operazione sincrona (attendo che tutti i task asyncio terminino)

    # Salvataggio risultati su GCS
    batch_result_path = posixpath.join(res.gcs_batch_result_dir, f"{dataset_name}_result_{batch_id}.jsonl")
    result_blob = res.bucket.blob(batch_result_path)
    result_blob.upload_from_string(
        "\n".join(json.dumps(obj) for obj in batch_result_list),
        content_type="application/json"
    )

    res.logger.info(f"[CRW][app][run_batch] -> Parallel analysis completed: batch result file uploaded into '{batch_result_path}'")

    return {"status": "completed", "batch_id": batch_id, "batch_path": batch_result_path}
