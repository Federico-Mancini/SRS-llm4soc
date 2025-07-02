from google.auth.transport.requests import Request as GoogleRequest
from google.oauth2 import id_token
from utils.resource_manager import resource_manager as res


# Creazione header per chiamate al runner su Cloud Run
def get_auth_header(audience_url: str) -> dict:
    try:
        auth_req = GoogleRequest()
        token = id_token.fetch_id_token(auth_req, audience_url)
        return {"Authorization": f"Bearer {token}"}
    
    except Exception as e:
        res.logger.error(f"[VMS][auth_utils][get_auth_header] -> Failed to create authentication header: {str(e)}")
        raise
