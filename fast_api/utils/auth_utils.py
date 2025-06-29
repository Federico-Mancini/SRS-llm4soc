import httpx

from google.auth.transport.requests import Request as GoogleRequest
from google.oauth2 import id_token
from resource_manager import ResourceManager


res = ResourceManager()


# Creazione header per chiamate al runner su Cloud Run
def get_auth_header(audience_url: str) -> dict:
    try:
        auth_req = GoogleRequest()
        token = id_token.fetch_id_token(auth_req, audience_url)
        return {"Authorization": f"Bearer {token}"}
    
    except Exception as e:
        res.logger.error(f"[VMS][auth_utils][get_auth_header] Failed to create authentication header: {e}")
        raise


# Gestore chiamate al runner su Cloud Run
async def call_runner(method: str, url: str, json: dict = None, timeout: float = 50.0) -> dict:
    headers = get_auth_header(url)

    try:
        async with httpx.AsyncClient(timeout=timeout) as client:
            if method.upper() == "GET":
                response = await client.get(url, headers=headers)

            elif method.upper() == "POST":
                response = await client.post(url, headers=headers, json=json)

            else:
                raise ValueError("Metodo HTTP non supportato")
            
            res.logger.info(f"[VMS][auth_utils][call_runner] {response.status_code} - {response.text}")
            
            response.raise_for_status()
            return response.json()

    except httpx.RequestError as e:
        res.logger.error(f"[VMS][auth_utils][call_runner] Connection error ({type(e)}): {str(e)}")
        raise

    except httpx.HTTPStatusError as e:
        res.logger.error(f"[VMS][auth_utils][call_runner] Invalid HTTP response ({type(e)}): {str(e)}")
        raise

    except Exception as e:
        res.logger.error(f"[VMS][auth_utils][call_runner] Unknown error ({type(e)}): {str(e)}")
        raise