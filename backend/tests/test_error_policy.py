from unittest.mock import AsyncMock, Mock

import httpx
import pytest
from sqlalchemy.exc import OperationalError

from backend.payments.error_policy import is_retryable_error
from backend.payments.retry import handle_processing_failure


@pytest.mark.parametrize(
    ("status_code", "expected"),
    [
        (400, False),
        (401, False),
        (403, False),
        (404, False),
        (422, False),
        (408, True),
        (429, True),
        (500, True),
        (503, True),
    ],
)
def test_http_error_classification(status_code, expected):
    request = httpx.Request("POST", "https://example.com/webhook")
    response = httpx.Response(status_code, request=request)

    with pytest.raises(httpx.HTTPStatusError) as exc_info:
        response.raise_for_status()

    assert is_retryable_error(exc_info.value) is expected


def test_network_error_is_retryable():
    request = httpx.Request("POST", "https://example.com/webhook")
    error = httpx.ConnectError(
        "Connection failed",
        request=request,
    )

    assert is_retryable_error(error) is True


def test_database_operational_error_is_retryable():
    error = OperationalError(
        "SELECT 1",
        {},
        RuntimeError("Database unavailable"),
    )

    assert is_retryable_error(error) is True


def test_unexpected_error_is_not_retryable():
    assert is_retryable_error(ValueError("Invalid data")) is False


@pytest.mark.asyncio
async def test_non_retryable_error_goes_directly_to_dlq(
    monkeypatch,
):
    publish_mock = AsyncMock()
    monkeypatch.setattr(
        "backend.payments.retry.broker.publish",
        publish_mock,
    )

    msg = Mock()
    msg.headers = {"retry_count": 0}
    msg.ack = AsyncMock()
    msg.nack = AsyncMock()
    msg.reject = AsyncMock()

    request = httpx.Request("POST", "https://example.com/webhook")
    response = httpx.Response(400, request=request)

    with pytest.raises(httpx.HTTPStatusError) as exc_info:
        response.raise_for_status()

    await handle_processing_failure(
        {"payment_id": "test-id"},
        msg,
        exc_info.value,
    )

    msg.reject.assert_awaited_once_with(requeue=False)
    msg.ack.assert_not_awaited()
    msg.nack.assert_not_awaited()
    publish_mock.assert_not_awaited()
