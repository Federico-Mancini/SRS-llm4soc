from fastapi import FastAPI, Request
from analyze_batch import analyze_batch_async


app = FastAPI()


@app.post("/run")
async def run_batch(req: Request):
    data = await req.json()
    batch_file = data["batch_file"]
    
    print(f"Ricevuto batch: {batch_file}")

    await analyze_batch_async(batch_file)
    
    return {"status": "ok", "batch": batch_file}
