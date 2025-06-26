import json, time, asyncio, datetime
import utils.vertexai_config_OLD as vxc

from utils.gcs_utils import upload_to_gcs
from utils.config import MAX_CONCURRENT_REQUESTS, RESULT_FILENAME, RESULT_PATH, ALERTS_PATH, CACHE_FILENAME, CACHE_PATH


# Vertex AI configuration
vxc.init()
model = vxc.get_model()
gen_conf = vxc.get_generation_config()


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

'''
# --- Elaborazione dati sincrona ----------------
def analyze_alert(i, alert) -> dict:
    prompt = build_prompt(alert)

    try:
        response = model.generate_content(prompt, generation_config=gen_conf)
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

def analyze_batch(input_path=ALERTS_PATH, output_path=RESULT_PATH) -> list:
    with open(input_path, "r") as f:
        alerts = [json.loads(line) for line in f if line.strip()]

    cache = load_cache()
    results = []

    for i, alert in enumerate(alerts, start=1):
        h = alert_hash(alert)           # calcolo dell'hash dell'alert corrente
        
        if h in cache:
            print(f"Cache hit for alert {i}")
            result = cache[h]           # caricamento del risultato salvato in cache
        else:
            result = analyze_alert(i, alert)
            
            if len(cache) >= MAX_CACHE_ENTRIES: # controllo spazio in cache
                cache.pop(next(iter(cache)))    # rimozione entry meno recente per far spazio a quella nuova
            
            cache[h] = {    # salvataggio in cache del risultato (solo i campi qui presenti sono rilevanti per la cache)
                "last_modified": time.time(),
                "class": result.get("class", "error"),
                "explanation": result.get("explanation", "error")
            }

        result["id"] = i                # (ri)assegnazione del valore "i" al campo "id"
        results.append(result)
        time.sleep(1)                   # rate limit di richieste a Gemini

    save_cache(cache)                   # scrittura dati in cache locale

    upload_to_gcs(RESULT_FILENAME)      # upload risultati in GCS (va qui e non in "main.py" perché è responsabilità di "analyze_batch" l'elaborazione e archiviazione dei dati)
    if is_cache_to_upload(CACHE_PATH):  # se True, la cache locale è più recente di quella remota e va aggiornata
        upload_to_gcs(CACHE_FILENAME)   # upload cache in GCS

    with open(output_path, "w") as f:   # scrittura risultati in locale (utile come backup in caso di GCS in down)
        json.dump(results, f, indent=2)

    print(f"Analisi completata. Risultati salvati in {output_path}")
    return results
'''

# --- Elaborazione dati asincrona ---------------
async def analyze_alert_async(i, alert, semaphore) -> dict:
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

async def analyze_batch_async(input_path=ALERTS_PATH, output_path=RESULT_PATH) -> list:

    # Apertura file in lettura
    with open(input_path, "r") as f:
        alerts = [json.loads(line) for line in f if line.strip()]

    results = []
    semaphore = asyncio.Semaphore(MAX_CONCURRENT_REQUESTS)

    ### START - Funzione interna per gestione cache
    async def process_alert(i, alert) -> dict:
        cached = load_alert_cache(alert_hash(alert))    # lettura cache

        if cached:
            print(f"Cache hit for alert {i}")
            result = cached.copy()
        else:
            result = await analyze_alert_async(i, alert, semaphore) # classificazione alert
            
            # Salvataggio cache (dati rilevanti di alert in file remoto dedicato)
            save_alert_cache({
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

    # Salvataggio risultati in remoto
    run_id = datetime.now().strftime("%Y%m%d-%H%M%S")   # ID istanza Cluod Run attualmente in esecuzione
    upload_to_gcs(f"results/result-{run_id}.json")      # salvataggio dei risultati su GCS (va qui e non in "main.py" perché è responsabilità di "analyze_batch" l'elaborazione e archiviazione dei dati)

    # Salvataggio risultati in locale
    with open(output_path, "w") as f:
        json.dump(results, f, indent=2)

    print(f"Analisi completata. Alert salvati in {output_path}")
    return results