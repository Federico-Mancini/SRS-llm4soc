import json, time, asyncio
import pandas as pd

from utils.resource_manager import resource_manager as res
from utils.cache_utils import alert_hash, cleanup_cache, download_cache, upload_cache


def build_prompt(alert) -> str:
    return f"""
Sei un assistente di sicurezza informatica. Ricevi un alert da un sistema IDS.
Il tuo compito è:
- Determinare se si tratta di un "false_positive" o di una "real_threat"
- Spiegare in italiano, con linguaggio chiaro ma tecnico, il motivo della classificazione
- Restituire una sola riga in formato JSON: {{ "class": ..., "explanation": ... }}

Esempio:
ALERT:
    {{
        "time": 1642213952,
        "name": "Wazuh: ClamAV database update",
        "ip": "172.17.131.81",
        "host": "mail",
        "short": "W-Sys-Cav",
        "time_label": "false_positive",
        "event_label": "-"
    }}
Risposta:
    {{"class": "false_positive", "explanation": "Aggiornamento del database ClamAV da host interno. Attività pianificata e legittima."}}

Ora analizza questo alert:
ALERT:
{json.dumps(alert, indent=2)}

Rispondi con un oggetto JSON singolo, su una sola riga.
""".strip()

# TODO: da testare
def build_prompt_optimized(alert) -> str:
    return f"""
        Classifica l'alert IDS come 'false_positive' o 'real_threat' e spiega brevemente il motivo, in italiano tecnico.
        Rispondi con una sola riga JSON: {"class":"...", "explanation":"..."}.\n\n
        'Esempio:\n'
        'ALERT:{"time":1642213952,"name":"Wazuh: ClamAV database update","ip":"172.17.131.81","host":"mail","short":"W-Sys-Cav","time_label":"false_positive","event_label":"-"}\n'
        'Risposta:{"class":"false_positive","explanation":"Aggiornamento ClamAV da host interno, attività legittima."}\n\n'
        'ALERT:{json.dumps(alert, separators=(",", ":"))}\nRisposta:'
    """


# Interpretazione risposta Gemini & costruzione JSON da restituire
def process_model_response(text: str, alert: dict, alert_id: int = 0) -> dict:
    text = text.strip()

    if "{" in text and "}" in text:
        text = text[text.find("{"):text.rfind("}") + 1]  # pulizia testo Gemini

    try:
        parsed = json.loads(text)
        return {
            "id": alert_id,
            "timestamp": alert.get("time", "n/a"),
            "class": parsed.get("class", "error"),
            "explanation": parsed.get("explanation", "Nessuna spiegazione")
        }
    
    except json.JSONDecodeError:
        return {
            "id": alert_id,
            "timestamp": alert.get("time", "n/a"),
            "class": "error",
            "explanation": f"Output non valido: {text}"
        }

# Analisi alert per l'endpoint '/chat'
def analyze_one_alert(alert) -> dict:
    prompt = build_prompt(alert)

    try:
        response = res.model.generate_content(prompt, generation_config=res.gen_conf)
        return process_model_response(response.text, alert)
    
    except Exception as e:
        return {
            "id": 0,
            "timestamp": alert.get("time", "n/a"),
            "class": "error",
            "explanation": f"{type(e).__name__}: {str(e)}"
        }

# Analisi asincrona i-esimo alert di batch
async def analyze_batch_alert(i, alert, semaphore) -> dict:
    prompt = build_prompt(alert)
    
    async with semaphore:
        try:
            response = await asyncio.to_thread(res.model.generate_content, prompt, generation_config=res.gen_conf)
            return process_model_response(response.text, alert, i)
        
        except Exception as e:
            return {
                "id": i,
                "timestamp": alert.get("time", "n/a"),
                "class": "error",
                "explanation": f"{type(e).__name__}: {str(e)}"
            }

# Analisi asincrona di batch
async def analyze_batch(batch_df: pd.DataFrame, batch_id: int, start_row: int) -> list[dict]:
    res.logger.info(f"[CRW][analyze_data][analyze_batch] -> Processing batch {batch_id} containing {batch_df.shape[0]} alerts")
    start_time = time.time()

    semaphore = asyncio.Semaphore(res.max_concurrent_requests)

    try:
        # Trasformazione dei record del dataframe in oggetti json
        alerts = [
            dict(zip(batch_df.columns, row))
            for row in batch_df.itertuples(index=False, name=None)
        ]

        # Parallelizzazione delle analisi sugli alert
        tasks = [
            analyze_batch_alert(start_row + i, alert, semaphore)
            for i, alert in enumerate(alerts, start=1)
        ]

        res.logger.info(f"[CRW][analyze_data][analyze_batch] Time to process batch {batch_id}: {time.time() - start_time:.2f} sec")

        return await asyncio.gather(*tasks)  # unione dei risultati dei singoli task: creazione file result del batch

    except Exception as e:
        res.logger.error(f"[CRW][analyze_data][analyze_batch] -> Error in batch {batch_id} ({type(e).__name__}): {str(e)}")
        raise

# Analisi asincrona di batch con gestione cache
async def analyze_batch_cached(batch_df: pd.DataFrame, batch_id: int, start_row: int) -> list[dict]:
    res.logger.info(f"[CRW][analyze_data][analyze_batch_cached] -> Processing batch {batch_id} containing {batch_df.shape[0]} alerts")
    start_time = time.time()

    try:
        # Estrazione lista nomi file cache (le graffe trasformano la lista in un set)
        existing_cache_hashes = {
            blob.name.split("/")[-1].replace(".json", "")  # es: "cache/H123.json" -> "H123"
            for blob in res.bucket.list_blobs(prefix=res.gcs_cache_dir + "/")
        }

        # Pulizia cache
        if len(existing_cache_hashes) > 1000:   # TODO: decidere quale soglia usare
            cleanup_cache()
        
        semaphore = asyncio.Semaphore(res.max_concurrent_requests)

        ### START - Funzione da parallelizzare (classificazione alert, gestione cache inclusa)
        async def process_alert(i, alert) -> dict:
            h = alert_hash(alert)

            if h in existing_cache_hashes:
                result = download_cache(h)
            else:
                result = await analyze_batch_alert(i, alert, semaphore)

                # Salvataggio cache
                upload_cache(h, {
                    "last_modified": time.time(),
                    "class": result.get("class", "error"),
                    "explanation": result.get("explanation", "error")
                })

            result["id"] = i
            return result
        ### END

        # Creazione task asincroni, uno per alert
        alerts = [dict(zip(batch_df.columns, row)) for row in batch_df.itertuples(index=False, name=None)]  # trasformazione di ogni record del dataframe in un oggetto json
        tasks = [process_alert(start_row + i, alert) for i, alert in enumerate(alerts, start=1)]

        res.logger.info(f"[CRW][analyze_data][analyze_batch_cached] Time to process batch {batch_id}: {time.time() - start_time:.2f} sec")

        return await asyncio.gather(*tasks)  # unione dei risultati dei singoli task: creazione file result del batch

    except Exception as e:
        res.logger.error(f"[CRW][analyze_data][analyze_batch_cached] -> Error in batch {batch_id} ({type(e).__name__}): {str(e)}")
        raise
