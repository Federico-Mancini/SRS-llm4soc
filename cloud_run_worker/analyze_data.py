import json, time, asyncio
import pandas as pd
import utils.gcs_utils as gcs
import utils.metrics_utils as mtr

from utils.resource_manager import resource_manager as res
from utils.cache_utils import alert_hash, cleanup_cache, download_cache, upload_cache


# F01 - Costruzione prompt per richiesta Gemini
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
    {{
        "class": "false_positive",
        "explanation": "Aggiornamento del database ClamAV da host interno. Attività pianificata e legittima."
    }}

Ora analizza questo alert:
ALERT:
{json.dumps(alert, indent=2)}

Rispondi con un oggetto JSON singolo, su una sola riga.
""".strip()

# F01B - TODO: da testare
def build_prompt_optimized(alert) -> str:
    return f"""
        Classifica l'alert IDS come 'false_positive' o 'real_threat' e spiega brevemente il motivo, in italiano tecnico.
        Rispondi con una sola riga JSON: {"class":"...", "explanation":"..."}.\n\n
        'Esempio:\n'
        'ALERT:{"time":1642213952,"name":"Wazuh: ClamAV database update","ip":"172.17.131.81","host":"mail","short":"W-Sys-Cav","time_label":"false_positive","event_label":"-"}\n'
        'Risposta:{"class":"false_positive","explanation":"Aggiornamento ClamAV da host interno, attività legittima."}\n\n'
        'ALERT:{json.dumps(alert, separators=(",", ":"))}\nRisposta:'
    """


# F02 - Interpretazione risposta Gemini & costruzione JSON da restituire
def process_model_response(text: str, alert: dict, alert_id: int = 0) -> dict:
    text = text.strip()

    if "{" in text and "}" in text:
        text = text[text.find("{"):text.rfind("}") + 1]  # pulizia testo Gemini

    try:
        parsed = json.loads(text)
        return {
            "id": alert_id,
            "timestamp": alert.get("time", res.not_available),
            "class": parsed.get("class", "error"),
            "explanation": parsed.get("explanation", "Nessuna spiegazione")
        }
    
    except json.JSONDecodeError as e:
        msg = text[:200].replace("\n", " ").replace("\"", "'")  # visualizzazione dei primi 200 caratteri, leggermente formattati per leggibilità
        res.logger.warning(f"[data|F03]\t\t-> Failed to parse JSON: {str(e)} | Response: {msg}")

        return {
            "id": alert_id,
            "timestamp": alert.get("time", res.not_available),
            "class": "error",
            "explanation": f"Output non valido: {text}"
        }

# F03 - Analisi asincrona i-esimo alert di batch
async def analyze_batch_alert(i: int, alert: dict, semaphore) -> dict:
    prompt = build_prompt(alert)
    
    async with semaphore:
        try:
            response = await asyncio.wait_for(
                asyncio.to_thread(res.model.generate_content, prompt, generation_config=res.gen_conf),
                timeout=60
            )
            return process_model_response(response.text, alert, i)
        
        except asyncio.TimeoutError:
            return {
                "batch_id": i,
                "timestamp": alert.get("time", res.not_available),
                "class": "error",
                "explanation": "Timeout: il modello ha impiegato troppo tempo per rispondere (impostare eventualmente un timeout maggiore)"
            }
    
        except Exception as e:
            return {
                "batch_id": i,
                "timestamp": alert.get("time", res.not_available),
                "class": "error",
                "explanation": f"{type(e).__name__}: {str(e)}"
            }


# F04 - Analisi asincrona di batch
async def analyze_batch(batch_df: pd.DataFrame, batch_id: int, start_row: int, dataset_name: str) -> list[dict]:
    res.logger.info(f"[data|F04]\t\t-> Processing batch {batch_id} containing {batch_df.shape[0]} alerts")
    timer_start, timestamp_start = mtr.init_monitoring()

    batch_size = len(batch_df)
    concurrency = min(batch_size, res.max_concurrent_requests)   # in caso di pochi alert (es: 3) evito l'apertura di 16 thread (='max_concurrent_requests' attuale)
    semaphore = asyncio.Semaphore(concurrency)

    try:
        # Trasformazione dei record del dataframe in lista di oggetti json
        alerts = batch_df.to_dict(orient='records') 

        # Parallelizzazione delle analisi sugli alert
        tasks = [
            analyze_batch_alert(start_row + i - 1, alert, semaphore)    # il -1 fa in modo che gli ID partano da 0
            for i, alert in enumerate(alerts, start=1)
        ]

        results = await asyncio.gather(*tasks)  # unione dei risultati dei singoli task: creazione file result del batch
        
        # Metriche
        metrics = mtr.finalize_monitoring(timer_start, timestamp_start, batch_id, batch_size, concurrency)
        metrics_path = gcs.get_blob_path(res.gcs_batch_metrics_dir, dataset_name, f"metrics_{batch_id}", "jsonl")
        await gcs.upload_as_jsonl(metrics_path, [metrics]) # NB: passare le metriche dentro una lista
        
        res.logger.info(f"[data|F04]\t\t-> Batch {batch_id}, time elapsed: {metrics['time_sec']}s")

        return results

    except Exception as e:
        res.logger.error(f"[data|F04]\t\t-> Error in batch {batch_id} ({type(e).__name__}): {str(e)}")
        raise

# F04B - Analisi asincrona di batch con gestione cache (OUTDATED!)
async def analyze_batch_cached(batch_df: pd.DataFrame, batch_id: int, start_row: int, dataset_name: str) -> list[dict]:
    res.logger.info(f"[data|F04B]\t\t-> Processing batch {batch_id} containing {batch_df.shape[0]} alerts")
    start = mtr.init_monitoring()

    concurrency = min(len(batch_df), res.max_concurrent_requests)   # in caso di pochi alert (es: 3) evito l'apertura di 16 thread (='max_concurrent_requests' attuale)
    semaphore = asyncio.Semaphore(concurrency)

    try:
        # Estrazione lista nomi file cache (le graffe trasformano la lista in un set)
        existing_cache_hashes = {
            blob.name.split("/")[-1].replace(".json", "")  # es: "cache/H123.json" -> "H123"
            for blob in res.bucket.list_blobs(prefix=res.gcs_cache_dir + "/")
        }

        # Pulizia cache
        if len(existing_cache_hashes) > 1000:   # TODO: decidere quale soglia usare
            cleanup_cache()
        
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

        # Trasformazione dei record del dataframe in lista di oggetti json
        alerts = batch_df.to_dict(orient='records')

        # Parallelizzazione delle analisi sugli alert
        tasks = [
            process_alert(start_row + i, alert)
            for i, alert in enumerate(alerts, start=1)
        ]
    
        results = await asyncio.gather(*tasks)  # unione dei risultati dei singoli task: creazione file result del batch
        # Metriche
        metrics = mtr.finalize_monitoring(start, batch_id, len(batch_df))
        metrics_path = f"{res.gcs_batch_metrics_dir}/{dataset_name}_batch_{batch_id}.jsonl"
        await gcs.upload_as_jsonl(metrics_path, [metrics]) # NB: passare le metriche dentro una lista
        
        res.logger.info(f"[data|F04B]\t\t-> Batch {batch_id}, Time elapsed: {metrics['time_sec']}s, RAM usage: {metrics['ram_mb']}MB")
        
        return results

    except Exception as e:
        res.logger.error(f"[data|F04B]\t\t-> Error in batch {batch_id} ({type(e).__name__}): {str(e)}")
        raise



# F05 - Analisi quesito utente per l'endpoint '/chat'
def analyze_chat_question(question: str, alerts: list[dict] | dict):
    try:
        alerts_str = alerts if isinstance(alerts, str) else json.dumps(alerts, indent=2, ensure_ascii=False)
        full_prompt = (
            f"Domanda: {question}\n\n"
            f"Alert selezionati:\n{alerts_str}\n\n"
            "Fornisci una risposta testuale, tenendo conto sia della domanda che del contesto degli alert."
        )

        return res.model.generate_content(full_prompt, generation_config=res.gen_conf).text

    except Exception as e:
        res.logger.error(f"[data|F05]\t\t-> Failed to generate a response ({type(e).__name__}): {str(e)}")
        raise
