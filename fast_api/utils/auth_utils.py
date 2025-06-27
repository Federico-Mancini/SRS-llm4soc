import logging, httpx

from google.auth.transport.requests import Request as GoogleRequest
from google.oauth2 import id_token


# Creazione header per chiamate al runner su Cloud Run
def get_auth_header(audience_url: str) -> dict:
    try:
        auth_req = GoogleRequest()
        token = id_token.fetch_id_token(auth_req, audience_url)
        return {"Authorization": f"Bearer {token}"}
    
    except Exception as e:
        logging.error(f"[auth_utils] Errore in creazione header di autenticazione: {e}")
        raise


# Gestore chiamate al runner su Cloud Run
async def call_runner(method: str, url: str, json: dict = None, timeout: float = 10.0) -> dict:
    headers = get_auth_header(url)

    try:
        async with httpx.AsyncClient(timeout=timeout) as client:
            if method.upper() == "GET":
                response = await client.get(url, headers=headers)

            elif method.upper() == "POST":
                response = await client.post(url, headers=headers, json=json)

            else:
                raise ValueError("Metodo HTTP non supportato")
            
            logging.info(f"[{response.status_code}] Response text: {response.text}")
            
            response.raise_for_status()
            return response.json()

    except httpx.RequestError as e:
        logging.error(f"[auth_utils|call_runner] Errore di connessione: {e}")
        raise

    except httpx.HTTPStatusError as e:
        logging.error(f"[auth_utils|call_runner] Risposta HTTP non valida: {e.response.status_code} - {e.response.text}")
        raise

    except Exception as e:
        logging.error(f"[auth_utils|call_runner] Errore sconosciuto: {e}")
        raise