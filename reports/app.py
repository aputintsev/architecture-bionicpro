import os
import logging
from datetime import date

import httpx
import clickhouse_connect
from fastapi import FastAPI, HTTPException, Depends
from fastapi.middleware.cors import CORSMiddleware
from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

app = FastAPI(title="Reports Service")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["GET"],
    allow_headers=["Authorization"],
)

AUTH_SERVICE_URL = os.getenv("AUTH_SERVICE_URL", "http://auth:8001")
CLICKHOUSE_HOST = os.getenv("CLICKHOUSE_HOST", "clickhouse")
CLICKHOUSE_PORT = int(os.getenv("CLICKHOUSE_PORT", "8123"))

bearer_scheme = HTTPBearer()


def _get_clickhouse():
    return clickhouse_connect.get_client(host=CLICKHOUSE_HOST, port=CLICKHOUSE_PORT)


async def _get_current_user(
    credentials: HTTPAuthorizationCredentials = Depends(bearer_scheme),
) -> dict:
    """Validate token through auth-service and return user info."""
    async with httpx.AsyncClient() as client:
        resp = await client.get(
            f"{AUTH_SERVICE_URL}/auth/verify-token",
            headers={"Authorization": f"Bearer {credentials.credentials}"},
            timeout=5,
        )
    if resp.status_code == 401:
        raise HTTPException(status_code=401, detail="Not authenticated")
    if resp.status_code != 200:
        logger.error("Auth service error: %s %s", resp.status_code, resp.text)
        raise HTTPException(status_code=502, detail="Auth service unavailable")
    return resp.json()


@app.get("/reports")
async def get_report(user: dict = Depends(_get_current_user)):
    """Return the pre-aggregated report for the authenticated user from ClickHouse."""
    email = user.get("email")
    if not email:
        raise HTTPException(status_code=400, detail="User email not found in token")

    today = date.today().isoformat()

    try:
        ch = _get_clickhouse()
        result = ch.query(
            """
            SELECT
                customer_id,
                prosthetic_id,
                report_date,
                customer_name,
                customer_email,
                total_events,
                avg_response_time_ms,
                min_response_time_ms,
                max_response_time_ms,
                avg_battery_level,
                avg_signal_quality,
                anomaly_count,
                most_common_movement
            FROM customer_report_mart
            WHERE customer_email = {email:String}
              AND report_date <= {today:String}
            ORDER BY report_date DESC
            """,
            parameters={"email": email, "today": today},
        )
    except Exception as exc:
        logger.error("ClickHouse query error: %s", exc)
        raise HTTPException(status_code=503, detail="OLAP database unavailable") from exc

    columns = [
        "customer_id", "prosthetic_id", "report_date", "customer_name",
        "customer_email", "total_events", "avg_response_time_ms",
        "min_response_time_ms", "max_response_time_ms",
        "avg_battery_level", "avg_signal_quality",
        "anomaly_count", "most_common_movement",
    ]

    rows = []
    for row in result.result_rows:
        entry = dict(zip(columns, row))
        if hasattr(entry["report_date"], "isoformat"):
            entry["report_date"] = entry["report_date"].isoformat()
        rows.append(entry)

    if not rows:
        return {
            "user": user.get("preferred_username"),
            "email": email,
            "message": "No report data available yet. Airflow processes data every 15 minutes.",
            "rows": [],
        }

    return {
        "user": user.get("preferred_username"),
        "email": email,
        "rows": rows,
    }


@app.get("/health")
async def health():
    return {"status": "ok"}


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
