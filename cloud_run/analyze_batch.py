import json, time, asyncio, datetime

from utils.cache_utils import alert_hash, download_alert_cache, upload_alert_cache
from utils.resource_manager import ResourceManager


res = ResourceManager()


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

# UNUSED (ancora da testare)
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


def analyze_single_alert(alert) -> dict:
    res.initialize()

    prompt = build_prompt(alert)

    try:
        response = res.model.generate_content(prompt, generation_config=res.gen_conf)
        text = response.text.strip()

        if "{" in text and "}" in text:
            text = text[text.find("{") : text.rfind("}") + 1]  # pulizia testo Gemini

        try:
            parsed = json.loads(text)
            return build_alert_entry(0, alert.get("time", "n/a"), parsed.get("class", "error"), parsed.get("explanation", "Nessuna spiegazione"))

        except json.JSONDecodeError:
            return build_alert_entry(0, alert.get("time", "n/a"), "error", f"Output non valido: {text}")
        
    except Exception as e:
        return build_alert_entry(0, alert.get("time", "n/a"), "error", str(e))


# Funzione interna (privata), usata da analyze_batch_async
async def analyze_alert_async(i, alert, semaphore, model, gen_conf) -> dict:
    prompt = build_prompt(alert)

    async with semaphore:
        try:
            response = await asyncio.to_thread(model.generate_content, prompt, generation_config=gen_conf)
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

async def analyze_batch_async(batch_path: str) -> list:
    res.initialize()

    blob = res.bucket.blob(batch_path)

    # Lettura del file batch '.jsonl'
    lines = blob.download_as_text().splitlines()
    alerts = [json.loads(line) for line in lines if line.strip()]

    semaphore = asyncio.Semaphore(res.max_concurrent_requests)
    results = []

    ### START - Funzione interna per gestione cache
    async def process_alert(i, alert) -> dict:
        cached = download_alert_cache(alert_hash(alert))    # lettura cache

        if cached:
            print(f"Cache hit for alert {i}")
            result = cached.copy()
        else:
            result = await analyze_alert_async(i, alert, semaphore, res.model, res.gen_conf) # classificazione alert
            
            # Salvataggio cache (dati rilevanti di alert in file remoto dedicato)
            upload_alert_cache({
                "last_modified": time.time(),
                "class": result.get("class", "error"),
                "explanation": result.get("explanation", "error")
            })

        result["id"] = i
        return result
    ### END

    # Creazione task asincrono per ogni alert
    tasks = [process_alert(i, alert) for i, alert in enumerate(alerts, start=1)]
    results = await asyncio.gather(*tasks)              # lista in cui salvare i risultati ottenuti da elaborazione parallela

    # Salvataggio risultati su GCS
    run_id = datetime.now().strftime("%Y%m%d-%H%M%S")   # ID istanza Cluod Run attualmente in esecuzione
    result_path = f"{res.gcs_result_dir}/result-{run_id}.json"
    
    res.bucket.blob(result_path).upload_from_string(json.dumps(results, indent=2))

    print(f"File {result_path} salvato su GCS")
    return results
