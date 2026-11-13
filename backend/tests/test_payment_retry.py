from unittest.mock import AsyncMock, Mock

import httpx
import pytest
from sqlalchemy.exc import OperationalError

from backend.payments.retry import handle_processing_failure


@pytest.fixture(autouse=True)
def choose_first_delay(monkeypatch):
    monkeypatch.setattr(
        "backend.payments.retry.random.choice", lambda choices: choices[0]
    )


def make_transient_error(stage: str) -> Exception:
    if stage == "database":
        return OperationalError(
            "SELECT 1",
            {},
            RuntimeError("Database unavailable"),
        )

    request = httpx.Request(
        "POST",
        "https://example.com/webhook",
    )
    return httpx.ConnectError(
        "Connection failed",
        request=request,
    )


def make_message(
    db_retry_count: int = 0,
    webhook_retry_count: int = 0,
) -> Mock:
    msg = Mock()
    msg.headers = {
        "db_retry_count": db_retry_count,
        "webhook_retry_count": webhook_retry_count,
    }
    msg.ack = AsyncMock()
    msg.nack = AsyncMock()
    msg.reject = AsyncMock()
    return msg


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("stage", "retry_count", "routing_key"),
    [
        ("database", 0, "payment.retry.1s"),
        ("database", 1, "payment.retry.2s"),
        ("webhook", 0, "payment.retry.1s"),
        ("webhook", 1, "payment.retry.2s"),
    ],
)
async def test_retry_published_before_ack(
    monkeypatch,
    stage,
    retry_count,
    routing_key,
):
    events = []

    async def publish_message(*args, **kwargs):
        events.append("publish")

    async def acknowledge():
        events.append("ack")

    publish_mock = AsyncMock(side_effect=publish_message)
    monkeypatch.setattr(
        "backend.payments.retry.broker.publish",
        publish_mock,
    )

    msg = make_message()
    msg.ack = AsyncMock(side_effect=acknowledge)

    counter_key = "db_retry_count" if stage == "database" else "webhook_retry_count"
    msg.headers[counter_key] = retry_count

    data = {"payment_id": "test-id"}

    await handle_processing_failure(
        data,
        msg,
        make_transient_error(stage),
    )

    publish_mock.assert_awaited_once()

    kwargs = publish_mock.await_args.kwargs
    assert kwargs["message"] == data
    assert kwargs["routing_key"] == routing_key
    assert kwargs["headers"] == {
        **msg.headers,
        counter_key: retry_count + 1,
    }
    assert kwargs["persist"] is True
    assert kwargs["mandatory"] is True

    msg.ack.assert_awaited_once()
    msg.nack.assert_not_awaited()
    msg.reject.assert_not_awaited()

    assert events == ["publish", "ack"]


@pytest.mark.asyncio
async def test_retry_publish_failure_requeues_original(
    monkeypatch,
):
    publish_mock = AsyncMock(side_effect=RuntimeError("Broker unavailable"))
    monkeypatch.setattr(
        "backend.payments.retry.broker.publish",
        publish_mock,
    )

    msg = make_message()

    await handle_processing_failure(
        {"payment_id": "test-id"},
        msg,
        make_transient_error("webhook"),
    )

    msg.nack.assert_awaited_once_with(requeue=True)
    msg.ack.assert_not_awaited()
    msg.reject.assert_not_awaited()


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "stage",
    ["database", "webhook"],
)
async def test_retry_exhausted_goes_to_dlq(
    monkeypatch,
    stage,
):
    publish_mock = AsyncMock()
    monkeypatch.setattr(
        "backend.payments.retry.broker.publish",
        publish_mock,
    )

    if stage == "database":
        msg = make_message(db_retry_count=2)
    else:
        msg = make_message(webhook_retry_count=2)

    await handle_processing_failure(
        {"payment_id": "test-id"},
        msg,
        make_transient_error(stage),
    )

    msg.reject.assert_awaited_once_with(requeue=False)
    msg.ack.assert_not_awaited()
    publish_mock.assert_not_awaited()


@pytest.mark.asyncio
async def test_webhook_has_independent_retry_limit(
    monkeypatch,
):
    publish_mock = AsyncMock()
    monkeypatch.setattr(
        "backend.payments.retry.broker.publish",
        publish_mock,
    )

    # Database has already failed twice.
    msg = make_message(
        db_retry_count=2,
        webhook_retry_count=0,
    )

    await handle_processing_failure(
        {"payment_id": "test-id"},
        msg,
        make_transient_error("webhook"),
    )

    kwargs = publish_mock.await_args.kwargs

    assert kwargs["routing_key"] == "payment.retry.1s"
    assert kwargs["headers"] == {
        "db_retry_count": 2,
        "webhook_retry_count": 1,
    }

    msg.ack.assert_awaited_once()
    msg.reject.assert_not_awaited()


@pytest.mark.asyncio
async def test_database_retries_do_not_reduce_webhook_attempts(
    monkeypatch,
):
    publish_mock = AsyncMock()
    monkeypatch.setattr(
        "backend.payments.retry.broker.publish",
        publish_mock,
    )

    data = {"payment_id": "test-id"}
    headers = {}

    stages = [
        "database",
        "database",
        "webhook",
        "webhook",
        "webhook",
    ]

    for index, stage in enumerate(stages):
        msg = make_message()
        msg.headers = headers.copy()

        await handle_processing_failure(
            data,
            msg,
            make_transient_error(stage),
        )

        if index < 4:
            msg.ack.assert_awaited_once()
            msg.reject.assert_not_awaited()

            headers = publish_mock.await_args.kwargs["headers"].copy()
        else:
            msg.reject.assert_awaited_once_with(requeue=False)
            msg.ack.assert_not_awaited()

    assert publish_mock.await_count == 4

    assert headers == {
        "db_retry_count": 2,
        "webhook_retry_count": 2,
    }
