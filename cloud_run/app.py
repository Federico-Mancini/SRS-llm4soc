# CRR: Cloud Run Runner
# (il runner va in "stand by" dopo un po'. A ogni primo utilizzo serve quindi inviargli una richiesta dummy per svegliarlo)

import json, httpx, asyncio
import utils.gcs_utils as gcs

from fastapi import FastAPI, Request, Query, HTTPException
from utils.resource_manager import resource_manager as res
from analyze_batch import analyze_single_alert, analyze_gcs_batch


app = FastAPI()


@app.on_event("startup")
async def startup_event():
    res.logger.info("[CRR][app][startup_event] Cloud Run Runner - Status: running")


# --- Endpoints ---------------------------------
@app.get("/")
def check_status():
    return {"status": "running"}


# Analisi di un singolo alert (passato tramite richiesta)
#NB: utilizzato per la funzionalità "chat"
@app.post("/run-alert")
async def run_alert(req: Request):
    data = await req.json()
    alert = data["alert"]

    if not alert:
        res.logger.error(f"[CRR][runner][run_alert] Missing 'alert' field from request body")
        raise HTTPException(status_code=400, detail="Campo 'alert' mancante")

    return analyze_single_alert(alert)


# Analisi di un singolo batch remoto (path GCS passato tramite richiesta)
# NB: batch ottenuto da suddivisione di dataset avviata dall'endpoint /run-dataset
@app.get("/run-batch")
async def run_batch(dataset_filename: str = Query(...), batch_path: str = Query(...)):
    try:
        res.logger.info(f"[CRR][runner][run_batch] Executing analysis of {batch_path}, from dataset {dataset_filename}")
        result_path = await analyze_gcs_batch(dataset_filename, batch_path)

        return {
            "message": "Batch elaborato correttamente",
            "batch": batch_path,
            "result_path": result_path
        }
    
    except HTTPException as http_err:
        res.logger.error(f"[CRR][runner][run_batch] Connection error ({type(e)}): {str(e)}")
        raise http_err

    except Exception as e:
        res.logger.error(f"[CRR][runner][run_batch] Failed to analyze batch {batch_path} ({type(e)}): {str(e)}")
        raise HTTPException(status_code=500, detail=f"Errore analisi batch: {str(e)}")


# Analisi di un dataset remoto (nome file GCS passato tramite richiesta)
# NB: la risposta è restituita non appena viene inviata la richiesta d'analisi dell'ultimo batch
# STEP: svuotamento directory batch -> suddivisione in batch -> analisi di batch -> analisi degli alert di ogni batch -> restituzione risultati in file separati per batch -> (cloud trigger) merge in unico 'result.json'
@app.get("/run-dataset")
async def run_dataset(dataset_filename: str = Query(...)):
    try:
        # Pulizia e preparazione
        gcs.empty_gcs_dir(res.gcs_batch_dir)
        gcs.empty_gcs_dir(res.gcs_batch_result_dir)

        # Suddivisione dataset in batch
        batch_paths = gcs.split_dataset(dataset_filename)
        semaphore = asyncio.Semaphore(res.max_concurrent_requests)

        # Lista dei risultati per il file di log
        log_entries = []

        async def retransmit_req(batch_path: str, index: int):    
            async with semaphore:
                try:
                    await run_batch(dataset_filename, batch_path)
                    log_entries.append({"batch": batch_path, "status": "ok"})
                
                except Exception as e:
                    res.logger.error(f"[CRR][app][run_dataset] -> Batch {index} exception: {e}")
                    log_entries.append({
                        "batch": batch_path,
                        "status": "error",
                        "detail": f"{e.response.status_code} {e.response.reason_phrase}"
                    })

        # Funzione background per invio richieste e salvataggio log
        async def launch_batches_and_log():
            tasks = [
                retransmit_req(batch_path, i)
                for i, batch_path in enumerate(batch_paths)
            ]
            await asyncio.gather(*tasks)

            # Salvataggio file log su GCS
            log_path = f"batch_log.json"
            res.bucket.blob(log_path).upload_from_string(json.dumps(log_entries, indent=2))
            res.logger.info(f"[CRR][app][run_dataset] -> Batch log uploaded into {log_path}")

        # Lancio del task asincrono in background
        asyncio.create_task(launch_batches_and_log())

        # Risposta immediata al client
        return {"message": "Analisi avviata in background", "batches": batch_paths}
        # NB: non attendo la fine dell'elaborazione dei batch, ma solo la loro generazione a seguito della suddivisione del dataset

    except Exception as e:
        msg = f"[CRR][app][run_dataset] -> Unknown error ({type(e)}): {str(e)}"
        res.logger.error(msg)
        raise HTTPException(status_code=500, detail=msg)