# CRW: Cloud Run Worker

import asyncio
import utils.gcs_utils as gcs
import utils.metrics_utils as mtr

from fastapi import FastAPI, HTTPException, Request
from concurrent.futures import ThreadPoolExecutor
from utils.resource_manager import resource_manager as res
from analyze_data import analyze_chat_question, analyze_batch, analyze_batch_cached


app = FastAPI()

executor = ThreadPoolExecutor(max_workers=16)
asyncio.get_event_loop().set_default_executor(executor) # aumento del limite massimo di thread concorrenti di asyncio


# Endpoint dedicato alla ricezione di richieste anomale dirette alla root del worker
@app.api_route("/", methods=["GET", "POST"])
async def block_root():
    raise HTTPException(status_code=404, detail="Invalid endpoint")


# Check di stato di server (VM) e worker (Cloud Run)
@app.get("/check-status")
async def check_status():
    return {"status": "running"}


# Aggiornamento variabili d'ambiente modificato a runtime (ad esempio dal benchmark)
@app.get("/reload-config")
async def check_status():
    res.reload_config()
    return {"message": "Resource manager reloaded"}


# Ricezione richieste d'analisi del batch i-esimo
# Operazioni: estrazione batch dal dataset remoto -> classificazione alert -> creazione file result temporaneo
@app.post("/run-batch")
async def run_batch(request: Request):
    try:
        body = await request.json()

        # Controllo ed estrazione campi
        required_fields = ["batch_id", "start_row", "end_row", "batch_size", "dataset_name", "dataset_path"]
        missing = [field for field in required_fields if field not in body or body[field] is None]

        if missing:
            msg = f"Missing required fields: {', '.join(missing)}"
            res.logger.warning(msg)
            raise HTTPException(status_code=500, detail=msg)

        batch_id, start_row, end_row, batch_size, dataset_name, dataset_path = (body[field] for field in required_fields)

        # Download e suddivisione del dataset
        batch_df = gcs.load_batch(dataset_path, start_row, end_row, batch_size)

        # Classificazione alert del batch
        batch_results = await analyze_batch(batch_df, batch_id, start_row, dataset_name)
        #batch_result_list = await analyze_batch_cached(batch_df, batch_id, start_row, dataset_name) # TODO: testare efficacia cache su dataset più grandi
        
        # Salvataggio risultati su GCS
        batch_results_path = gcs.get_blob_path(res.gcs_batch_result_dir, dataset_name, f"result_{batch_id}", "jsonl")
        await gcs.upload_as_jsonl(batch_results_path, batch_results)    # 'batch_results' è una lista di oggetti JSON

        # Calcolo numero errori di classificazione e aggiornamento metriche
        batch_metrics_path = gcs.get_blob_path(res.gcs_batch_metrics_dir, dataset_name, f"metrics_{batch_id}", "jsonl")
        updated_metrics = mtr.update_metrics(batch_results, len(batch_df), batch_metrics_path)
        print(f"Updated metricss: {updated_metrics}")
        await gcs.upload_as_jsonl(batch_metrics_path, updated_metrics)

        res.logger.info(f"[CRW][app][run_batch] -> Parallel analysis completed: batch result file uploaded into '{batch_results_path}'")

        return {
            "status": "completed",
            "batch_id": batch_id,
            "batch_path": batch_results_path
        }
    
    except Exception as e:
        msg = f"[CRW][app][run_batch] -> Error ({type(e).__name__}): {str(e)}"
        res.logger.error(msg)
        return {"detail": msg}
    

# Ricezione richieste d'analisi di un solo alert (da '/chat' di server)
@app.post("/run-chatbot")
async def run_alert(req: Request):
    data = await req.json()
    question = data["question"]
    alerts = data["alerts"]

    if not question or not alerts:
        msg = f"[CRR][runner][run_alert] -> Missing fields from request body"
        res.logger.error(msg)
        raise HTTPException(status_code=400, detail=msg)

    return {
        "explanation": analyze_chat_question(question, alerts)
    }
