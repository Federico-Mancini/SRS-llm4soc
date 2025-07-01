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

def build_alert_entry(i, t, c, e) -> dict:
    return {
        "id": i,
        "timestamp": t,
        "class": c,
        "explanation": e
    }


# Funzione interna (privata), usata da analyze_batch_async
async def analyze_alert_async(i, alert, semaphore) -> dict:
    prompt = build_prompt(alert)

    async with semaphore:
        try:
            response = await asyncio.to_thread(res.model.generate_content, prompt, generation_config=res.gen_conf)
            text = response.text.strip()

            if "{" in text and "}" in text:
                text = text[text.find("{") : text.rfind("}") + 1]   # pulizia testo Gemini

            try:
                parsed = json.loads(text)
                return build_alert_entry(i, alert.get("time", "n/a"), parsed.get("class", "error"), parsed.get("explanation", "Nessuna spiegazione"))

            except json.JSONDecodeError:
                return build_alert_entry(i, alert.get("time", "n/a"), "error", f"Output non valido: {text}")

        except Exception as e:
            return build_alert_entry(i, alert.get("time", "n/a"), "error", str(e))

async def analyze_batch(batch_df: pd.DataFrame, batch_id: int, batch_size: int) -> list[dict]:
    res.logger.info(f"[CRW][analyze_data][analyze_batch] -> Processing batch {batch_id} containing {batch_size} alerts")

    try:
        cleanup_cache() # TODO: vedere se c'è un punto migliore in cui eseguire la pulizia della cache

        semaphore = asyncio.Semaphore(res.max_concurrent_requests)

        ### START - Funzione da parallelizzare (classificazione alert, gestione cache inclusa)
        async def process_alert(i, alert) -> dict:
            cached = download_cache(alert_hash(alert))    # lettura cache

            if cached:
                result = cached.copy()
            else:
                result = await analyze_alert_async(i, alert, semaphore)
                
                # Salvataggio cache (dati rilevanti di alert in file remoto dedicato)
                upload_cache({
                    "last_modified": time.time(),
                    "class": result.get("class", "error"),
                    "explanation": result.get("explanation", "error")
                })

            result["id"] = i
            return result
        ### END

        # Creazione task asincroni, uno per alert
        res.logger.info(f"[CRW][analyze_data][analyze_batch] -> Launching {res.max_concurrent_requests} parallel analysis for {batch_size} alerts")

        alerts = [dict(zip(batch_df.columns, row)) for row in batch_df.itertuples(index=False, name=None)]  # trasformazione di ogni record del dataframe in un oggetto json
        tasks = [process_alert(i, alert) for i, alert in enumerate(alerts, start=1)]

        return await asyncio.gather(*tasks)  # unione dei risultati dei singoli task: creazione file result del batch

    except Exception as e:
        res.logger.error(f"[CRW][analyze_data][analyze_batch] -> Error in batch {batch_id} ({type(e).__name__}): {str(e)}")
        raise