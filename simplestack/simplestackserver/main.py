import logging
import os
from typing import Optional

from fastapi import FastAPI, Depends, HTTPException
from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials
from google.auth import jwt

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger('simplestack-server')

app = FastAPI()


def email_from_id_token(token):
    """Get email from id_token"""

    decoded = jwt.decode(token, verify=False)
    if 'email' in decoded.keys():
        logger.info(f"Using GCP token format, email={decoded['email']}")
        return decoded['email']
    elif 'upn' in decoded.keys():
        logger.info(f"Using Azure token format, email={decoded['upn']}")
        return decoded['upn']
    else:
        # Probably need to rethink what to raise here, but this is a temp hack.
        raise HTTPException(
            status_code=401, detail=f'No recognised email in auth token'
        )


def authenticate(
    token: Optional[HTTPAuthorizationCredentials] = Depends(
        HTTPBearer(auto_error=False)
    ),
) -> str:
    """Authorize with 'Authorization: Bearer $(token)'"""
    if token:
        return email_from_id_token(token.credentials)

    raise HTTPException(status_code=401, detail=f'No recognised auth mechanic')


gcp_path = os.getenv('GCP_PATH')
az_path = os.getenv('AZ_PATH')


@app.get('/')
def get_urls(whoami: str = Depends(authenticate)):
    return {'gcp': gcp_path, 'azure': az_path, 'whoami': whoami}


if __name__ == '__main__':
    import uvicorn

    uvicorn.run(app, host='0.0.0.0', port=int(os.getenv('PORT', '8000')), debug=True)
