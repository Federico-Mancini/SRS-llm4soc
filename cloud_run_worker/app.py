# CRW: Cloud Run Worker

import io, json, posixpath
import pandas as pd

from fastapi import FastAPI, HTTPException, Request
from utils.resource_manager import resource_manager as res
from analyze_data import analyze_one_alert, analyze_batch, analyze_batch_cached


app = FastAPI()


# Endpoint dedicato alla ricezione di richieste anomale dirette alla root del worker
@app.api_route("/", methods=["GET", "POST"])
async def block_root():
    raise HTTPException(status_code=404, detail="Invalid endpoint")


# Ricezione richieste d'analisi di un solo alert (da '/chat' di server)
@app.post("/run-alert")
async def run_alert(req: Request):
    data = await req.json()
    alert = data["alert"]

    if not alert:
        msg = f"[CRR][runner][run_alert] -> Missing 'alert' field from request body"
        res.logger.error(msg)
        raise HTTPException(status_code=400, detail=msg)

    return analyze_one_alert(alert)


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

    # Download e suddivisione del dataset
    data = res.bucket.blob(dataset_path).download_as_text()
    df = pd.read_json(io.StringIO(data), lines=True)
    batch_df = df.iloc[start_row:end_row]

    # Classificazione alert del batch
    batch_result_list = await analyze_batch(batch_df, batch_id, start_row)
    #batch_result_list = await analyze_batch_cached(batch_df, batch_id, start_row) # TODO: testare efficacia cache su dataset più grandi
    
    # Salvataggio risultati su GCS
    batch_result_path = posixpath.join(res.gcs_batch_result_dir, f"{dataset_name}_result_{batch_id}.jsonl")
    res.bucket.blob(batch_result_path).upload_from_string(
        "\n".join(json.dumps(obj) for obj in batch_result_list),
        content_type="application/json"
    )

    res.logger.info(f"[CRW][app][run_batch] -> Parallel analysis completed: batch result file uploaded into '{batch_result_path}'")

    return {"status": "completed", "batch_id": batch_id, "batch_path": batch_result_path}
