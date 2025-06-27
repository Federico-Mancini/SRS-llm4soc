import logging

from fastapi import FastAPI, Request
from analyze_batch import analyze_batch_async


logging.basicConfig(level=logging.INFO)
app = FastAPI()


@app.on_event("startup")
async def startup_event():
    logging.info("Runner avviato e pronto.")


@app.get("/")
def health():
    return {"status": "running"}


@app.post("/run")
async def run_batch(req: Request):
    data = await req.json()
    batch_file = data["batch_file"]
    
    print(f"Ricevuto batch: {batch_file}")

    await analyze_batch_async(batch_file)
    
    return {"status": "ok", "batch": batch_file}
