from unittest.mock import AsyncMock, Mock

import asyncpg
import httpx
import pytest
from sqlalchemy.dialects.postgresql.asyncpg import (
    AsyncAdapt_asyncpg_connection,
    AsyncAdapt_asyncpg_dbapi,
)
from sqlalchemy.exc import DBAPIError

from backend.core.broker import RETRY_QUEUES
from backend.payments.error_policy import get_retry_counter_key, is_retryable_error
from backend.payments.retry import handle_processing_failure


@pytest.mark.parametrize(
    "error_class,expected",
    [
        (asyncpg.SerializationError, True),
        (asyncpg.DeadlockDetectedError, True),
        (asyncpg.ConnectionDoesNotExistError, True),
        (asyncpg.UniqueViolationError, False),
        (asyncpg.CheckViolationError, False),
        (asyncpg.PostgresSyntaxError, False),
    ],
)
def test_actual_driver_exception_translation(error_class, expected):
    adapter = AsyncAdapt_asyncpg_dbapi(asyncpg)
    try:
        AsyncAdapt_asyncpg_connection._handle_exception_no_connection(
            adapter, error_class("test")
        )
    except Exception as error:  # noqa: BLE001 - exercise the real adapter's exception mapping
        wrapped = DBAPIError.instance("SELECT 1", {}, error, adapter.Error)
    assert is_retryable_error(wrapped) is expected
    assert get_retry_counter_key(wrapped) == "db_retry_count"


@pytest.mark.parametrize(
    "error,counter",
    [
        (ConnectionRefusedError(), "db_retry_count"),
        (TimeoutError(), "db_retry_count"),
        (httpx.ReadTimeout("deadline"), "webhook_retry_count"),
    ],
)
def test_connection_error_counters(error, counter):
    assert is_retryable_error(error)
    assert get_retry_counter_key(error) == counter


@pytest.mark.asyncio
@pytest.mark.parametrize("attempt", [0, 1])
@pytest.mark.parametrize("bucket", [0, 1, 2])
async def test_jitter_uses_every_bounded_bucket(monkeypatch, attempt, bucket):
    monkeypatch.setattr(
        "backend.payments.retry.random.choice", lambda choices: choices[bucket]
    )
    publish = AsyncMock()
    monkeypatch.setattr("backend.payments.retry.broker.publish", publish)
    message = Mock(
        headers={"webhook_retry_count": attempt}, ack=AsyncMock(), reject=AsyncMock()
    )
    await handle_processing_failure({}, message, httpx.ConnectError("test"))
    assert (
        publish.await_args.kwargs["routing_key"]
        == RETRY_QUEUES[attempt][bucket].routing_key
    )
    assert publish.await_args.kwargs["timeout"] == 5.0
    message.ack.assert_awaited_once()


@pytest.mark.asyncio
@pytest.mark.parametrize("count", [-1, 3, True, "1", None])
async def test_invalid_retry_counter_rejected(monkeypatch, count):
    publish = AsyncMock()
    monkeypatch.setattr("backend.payments.retry.broker.publish", publish)
    message = Mock(
        headers={"db_retry_count": count}, ack=AsyncMock(), reject=AsyncMock()
    )
    await handle_processing_failure({}, message, httpx.ConnectError("test"))
    message.reject.assert_awaited_once_with(requeue=False)
    publish.assert_not_awaited()
