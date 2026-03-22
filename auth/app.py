import os
import logging

import httpx
from fastapi import FastAPI, HTTPException, Depends
from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials
from jose import jwt, JWTError

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

app = FastAPI(title="Auth Service")

# --- Config ---
KEYCLOAK_URL = os.getenv("KEYCLOAK_URL", "http://keycloak:8080")
KEYCLOAK_REALM = os.getenv("KEYCLOAK_REALM", "reports-realm")
KEYCLOAK_JWKS_URL = (
    f"{KEYCLOAK_URL}/realms/{KEYCLOAK_REALM}/protocol/openid-connect/certs"
)

# Simple in-memory cache for JWKS (public keys don't change often)
_jwks_cache: dict | None = None

# --- Security ---
bearer_scheme = HTTPBearer()


async def _get_jwks() -> dict:
    """Fetch Keycloak public keys (JWKS) and cache them in memory."""
    global _jwks_cache
    if _jwks_cache is None:
        async with httpx.AsyncClient() as client:
            resp = await client.get(KEYCLOAK_JWKS_URL, timeout=10)
        if resp.status_code != 200:
            raise HTTPException(status_code=502, detail="Cannot fetch Keycloak JWKS")
        _jwks_cache = resp.json()
    return _jwks_cache


@app.get("/auth/verify-token")
async def verify_keycloak_token(
    credentials: HTTPAuthorizationCredentials = Depends(bearer_scheme),
):
    """Validate a Keycloak-issued token locally via JWKS and return user info.

    Uses local signature verification instead of the userinfo endpoint to avoid
    hostname mismatch (token iss=localhost:8080, internal call=keycloak:8080).
    """
    global _jwks_cache
    token = credentials.credentials

    # Try with cached keys first; on failure clear cache and retry once
    # (handles Keycloak key rotation).
    for attempt in range(2):
        jwks = await _get_jwks()
        try:
            payload = jwt.decode(
                token,
                jwks,
                algorithms=["RS256"],
                options={"verify_aud": False},
            )
            return {
                "sub": payload.get("sub"),
                "preferred_username": payload.get("preferred_username"),
                "email": payload.get("email"),
                "roles": payload.get("realm_access", {}).get("roles", []),
            }
        except JWTError:
            if attempt == 0:
                _jwks_cache = None  # invalidate cache and retry
            else:
                raise HTTPException(status_code=401, detail="Invalid or expired token")


@app.get("/health")
async def health():
    return {"status": "ok"}


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8001)
