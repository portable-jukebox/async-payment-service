import httpx
import pytest

from backend.main import app


@pytest.mark.asyncio
async def test_healthz_public_but_payments_remain_protected():
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app), base_url="http://test"
    ) as client:
        assert (await client.get("/healthz")).json() == {"status": "ok"}
        assert (await client.get("/health")).status_code == 401
        assert (await client.post("/api/v1/payments", json={})).status_code == 401
