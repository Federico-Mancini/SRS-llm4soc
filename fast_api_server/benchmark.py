# API dedicata all'esecuzione del benchmark (impossibile usare questi endpoint da dentro il server FastAPI senza entrare in deadlock)

import json

from fastapi import FastAPI, HTTPException, Query, BackgroundTasks
from fastapi.responses import JSONResponse
from utils.resource_manager import resource_manager as res
from utils.benchmark_utils import run_benchmark


DEFAULT_BATCH_SIZE_SUP = 250
DEFAULT_MAX_REQS_SUP = 16

app = FastAPI()
# Comando per lanciare il server:
#   uvicorn benchmark:app --host 0.0.0.0 --port 8001


# E01 - Esecuzione automatizzata di analisi dataset con parametrizzazione variabile
@app.get("/start-benchmark")
def start_benchmark(
    background_tasks: BackgroundTasks,
    dataset_filename: str = Query(...),
    batch_size_inf: int = Query(1),
    batch_size_sup: int = Query(DEFAULT_BATCH_SIZE_SUP),
    batch_size_step: int = Query(1),
    max_reqs_inf: int = Query(1),
    max_reqs_sup: int = Query(DEFAULT_MAX_REQS_SUP),
    max_reqs_step: int = Query(1)
):
    if batch_size_sup > DEFAULT_BATCH_SIZE_SUP:
        msg = f"[benchmark|E01]\t-> 'sup_batch_size' cannot exceed {DEFAULT_BATCH_SIZE_SUP}"
        res.logger.error(msg)
        raise HTTPException(status_code=400, detail=msg)
    if max_reqs_sup > DEFAULT_MAX_REQS_SUP:
        msg = f"[benchmark|E01]\t-> 'sup_max_reqs' cannot exceed {DEFAULT_MAX_REQS_SUP}"
        res.logger.error(msg)
        raise HTTPException(status_code=400, detail=msg)
    

    background_tasks.add_task(
        run_benchmark,
        dataset_filename,
        batch_size_inf,
        batch_size_sup,
        batch_size_step,
        max_reqs_inf,
        max_reqs_sup,
        max_reqs_step
    )

    return {"message": "Benchmark started in background. Keep an eye on the server log to spot eventual errors"}


# E02 - Terminazione benchmark in esecuzione
@app.get("/stop-benchmark")
def stop_benchmark():
    with open(res.vms_benchmark_stop_flag, "w") as f:   # creazione di un flag file per segnalare l'intenzione di terminare al benchmark
        f.write("stop")
    return {"status": "Benchmark interruption signal sent"}


# E03 - Monitoraggio stato del benchmark
@app.get("/benchmark-status")
def check_benchmark_status():
    try:
        with open(res.vms_benchmark_context_path, "r", encoding="utf-8") as f:
            state = json.load(f)
        return JSONResponse(content=state)
    except Exception as e:
        msg = f"[benchmark|E03]\t-> Failed to read benchmark context: {str(e)}"
        res.logger.error(msg)
        raise HTTPException(status_code=500, detail=msg)
