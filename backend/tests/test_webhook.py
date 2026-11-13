import json
from unittest.mock import ANY, AsyncMock, Mock

import httpx
import pytest

from backend.webhooks.service import deliver_webhook


@pytest.mark.asyncio
async def test_deliver_webhook_success():
    requests_count = 0

    def handler(request: httpx.Request) -> httpx.Response:
        nonlocal requests_count
        requests_count += 1

        assert request.method == "POST"
        assert str(request.url) == "https://example.com/webhook"
        assert request.headers["Idempotency-Key"] == "123"

        return httpx.Response(200)

    transport = httpx.MockTransport(handler)

    async with httpx.AsyncClient(transport=transport) as client:
        await deliver_webhook(
            client,
            "https://example.com/webhook",
            {"payment_id": "123", "status": "succeeded"},
        )

    assert requests_count == 1


@pytest.mark.asyncio
@pytest.mark.parametrize("status_code", [400, 429, 500, 503])
async def test_deliver_webhook_http_error_only_one_attempt(
    status_code,
):
    url = "http://test"
    payload = {"payment_id": "123", "status": "succeeded"}

    response = httpx.Response(
        status_code,
        request=httpx.Request("POST", url),
    )

    http_client = Mock()
    http_client.post = AsyncMock(return_value=response)

    with pytest.raises(httpx.HTTPStatusError) as exc_info:
        await deliver_webhook(http_client, url, payload)

    assert exc_info.value.response.status_code == status_code

    http_client.post.assert_awaited_once_with(
        url=url,
        content=json.dumps(payload, separators=(",", ":")).encode(),
        headers=ANY,
        follow_redirects=False,
    )


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "error",
    [
        httpx.ConnectError("Connection refused"),
        httpx.ReadTimeout("Timed out"),
    ],
)
async def test_deliver_webhook_network_error_only_one_attempt(
    error,
):
    url = "http://test"
    payload = {"payment_id": "123", "status": "succeeded"}

    http_client = Mock()
    http_client.post = AsyncMock(side_effect=error)

    with pytest.raises(type(error)):
        await deliver_webhook(http_client, url, payload)

    http_client.post.assert_awaited_once_with(
        url=url,
        content=json.dumps(payload, separators=(",", ":")).encode(),
        headers=ANY,
        follow_redirects=False,
    )


@pytest.mark.asyncio
async def test_repeated_webhook_uses_same_idempotency_key():
    requests = []

    def handler(request: httpx.Request) -> httpx.Response:
        requests.append(request)
        return httpx.Response(200)

    payload = {
        "payment_id": "payment-123",
        "status": "succeeded",
    }

    async with httpx.AsyncClient(
        transport=httpx.MockTransport(handler),
    ) as client:
        await deliver_webhook(
            client,
            "https://example.com/webhook",
            payload,
        )
        await deliver_webhook(
            client,
            "https://example.com/webhook",
            payload,
        )

    assert len(requests) == 2
    assert all(
        request.headers["Idempotency-Key"] == "payment-123" for request in requests
    )
