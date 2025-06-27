import logging

from fastapi import FastAPI, Request
from analyze_batch import analyze_single_alert, analyze_batch_async


logging.basicConfig(level=logging.INFO)
app = FastAPI()


@app.on_event("startup")
async def startup_event():
    logging.info("Cloud Run - runner avviato e pronto.")


# --- Endpoints ---------------------------------
@app.get("/")
def check_status():
    return {"status": "running"}


@app.post("/run-alert")
async def run_alert(req: Request):
    data = await req.json()
    alert = data["alert"]

    return analyze_single_alert(alert)


@app.post("/run-batch")
async def run_batch(req: Request):
    data = await req.json()
    batch_file = data["batch_file"]
    
    print(f"Ricevuto batch: {batch_file}")

    await analyze_batch_async(batch_file)
    
    return {"batch": batch_file}
