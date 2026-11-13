from fastapi import Depends, FastAPI
from fastapi.responses import Response
from prometheus_client import CONTENT_TYPE_LATEST

from backend.api.router import api_router
from backend.core.config import Settings
from backend.core.metrics import database_metrics
from backend.core.security import verify_api_key
from backend.db.session import async_session_factory

settings = Settings()
app = FastAPI(title=settings.app_name)
app.include_router(api_router)


@app.get("/metrics", include_in_schema=False)
async def metrics():
    return Response(
        await database_metrics(async_session_factory), media_type=CONTENT_TYPE_LATEST
    )


@app.get("/healthz", include_in_schema=False)
def healthz():
    return {"status": "ok"}


@app.get("/health", dependencies=[Depends(verify_api_key)])
def check_health():
    return {"status": "ok"}
