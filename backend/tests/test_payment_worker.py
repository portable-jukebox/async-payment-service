import json
from unittest.mock import ANY, AsyncMock, Mock
from uuid import uuid4

import httpx
import pytest
from sqlalchemy.exc import OperationalError
from sqlalchemy.ext.asyncio import async_sessionmaker

from backend.payments.models import Payment
from backend.payments.worker import process_payment


def make_message(retry_count: int = 0) -> Mock:
    msg = Mock()
    msg.headers = {
        "db_retry_count": 0,
        "webhook_retry_count": retry_count,
    }
    msg.ack = AsyncMock()
    msg.nack = AsyncMock()
    msg.reject = AsyncMock()
    return msg


@pytest.mark.parametrize(
    ("random_value", "expected_status"),
    [(0.5, "succeeded"), (0.95, "failed")],
)
@pytest.mark.asyncio
async def test_process_payment_idempotency(
    test_session, monkeypatch, random_value, expected_status
):
    random_mock = Mock(return_value=random_value)
    monkeypatch.setattr("backend.payments.service.random.random", random_mock)

    sleep_mock = AsyncMock()
    monkeypatch.setattr("backend.payments.worker.asyncio.sleep", sleep_mock)
    monkeypatch.setattr(
        "backend.payments.worker.random.uniform", Mock(return_value=2.5)
    )

    payment = Payment(
        amount=1000,
        currency="RUB",
        description="description",
        _metadata={"metadata": "metadata"},
        webhook_url="http://test",
        idempotency_key=f"test-payment-{expected_status}",
    )
    test_session.add(payment)
    await test_session.commit()

    test_factory = async_sessionmaker(test_session.bind, expire_on_commit=False)
    monkeypatch.setattr("backend.payments.worker.async_session_factory", test_factory)

    response = httpx.Response(
        status_code=200,
        request=httpx.Request("POST", "http://test"),
    )
    http_client = Mock()
    http_client.post = AsyncMock(return_value=response)

    first_msg = make_message()
    await process_payment({"payment_id": str(payment.id)}, first_msg, http_client)
    await test_session.refresh(payment)

    assert payment.status == expected_status
    assert payment.processed_at is not None
    timestamp = payment.processed_at
    first_msg.ack.assert_awaited_once()
    first_msg.nack.assert_not_awaited()
    first_msg.reject.assert_not_awaited()

    second_msg = make_message()
    await process_payment({"payment_id": str(payment.id)}, second_msg, http_client)
    await test_session.refresh(payment)

    assert payment.status == expected_status
    assert payment.processed_at == timestamp
    random_mock.assert_called_once()
    second_msg.ack.assert_awaited_once()
    second_msg.nack.assert_not_awaited()
    second_msg.reject.assert_not_awaited()

    assert http_client.post.await_count == 1
    http_client.post.assert_any_await(
        url="http://test",
        content=json.dumps(
            {"payment_id": str(payment.id), "status": expected_status},
            separators=(",", ":"),
        ).encode(),
        headers=ANY,
        follow_redirects=False,
    )
    assert sleep_mock.await_count == 1
    assert payment.webhook_delivered_at is not None


@pytest.mark.asyncio
async def test_process_payment_db_error_delegates_retry(monkeypatch):
    payment_id = uuid4()
    db_error = OperationalError("SELECT", {}, Exception("Database unavailable"))
    db_mock = AsyncMock(side_effect=db_error)
    retry_mock = AsyncMock()
    sleep_mock = AsyncMock()

    monkeypatch.setattr("backend.payments.worker.process_payment_in_db", db_mock)
    monkeypatch.setattr("backend.payments.worker.handle_processing_failure", retry_mock)
    monkeypatch.setattr("backend.payments.worker.asyncio.sleep", sleep_mock)
    monkeypatch.setattr(
        "backend.payments.worker.random.uniform", Mock(return_value=2.5)
    )
    monkeypatch.setattr(
        "backend.payments.worker.get_payment_state_in_db",
        AsyncMock(return_value=(True, False)),
    )

    http_client = Mock()
    http_client.post = AsyncMock()
    msg = make_message()
    data = {"payment_id": str(payment_id)}

    await process_payment(data, msg, http_client)

    db_mock.assert_awaited_once_with(payment_id)
    sleep_mock.assert_awaited_once_with(2.5)
    http_client.post.assert_not_awaited()
    retry_mock.assert_awaited_once()
    args = retry_mock.await_args.args
    assert args[0] == data
    assert args[1] is msg
    assert args[2] is db_error
    msg.ack.assert_not_awaited()
    msg.nack.assert_not_awaited()
    msg.reject.assert_not_awaited()


@pytest.mark.asyncio
async def test_process_payment_webhook_error_delegates_retry(monkeypatch):
    payment_id = uuid4()
    payload = {"payment_id": str(payment_id), "status": "succeeded"}
    db_mock = AsyncMock(return_value=("http://test", payload))
    retry_mock = AsyncMock()

    monkeypatch.setattr("backend.payments.worker.process_payment_in_db", db_mock)
    monkeypatch.setattr("backend.payments.worker.handle_processing_failure", retry_mock)
    monkeypatch.setattr("backend.payments.worker.asyncio.sleep", AsyncMock())
    monkeypatch.setattr(
        "backend.payments.worker.get_payment_state_in_db",
        AsyncMock(return_value=(False, False)),
    )

    response = httpx.Response(503, request=httpx.Request("POST", "http://test"))
    http_client = Mock()
    http_client.post = AsyncMock(return_value=response)
    msg = make_message(retry_count=1)
    data = {"payment_id": str(payment_id)}

    await process_payment(data, msg, http_client)

    db_mock.assert_awaited_once_with(payment_id)
    http_client.post.assert_awaited_once_with(
        url="http://test",
        content=json.dumps(payload, separators=(",", ":")).encode(),
        headers=ANY,
        follow_redirects=False,
    )
    retry_mock.assert_awaited_once()
    args = retry_mock.await_args.args
    assert args[0] == data
    assert args[1] is msg
    assert isinstance(args[2], httpx.HTTPStatusError)
    msg.ack.assert_not_awaited()


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "data",
    [{}, {"payment_id": "invalid-uuid"}, {"payment_id": None}],
)
async def test_process_payment_invalid_message_rejected(monkeypatch, data):
    db_mock = AsyncMock()
    sleep_mock = AsyncMock()
    monkeypatch.setattr("backend.payments.worker.process_payment_in_db", db_mock)
    monkeypatch.setattr("backend.payments.worker.asyncio.sleep", sleep_mock)

    http_client = Mock()
    http_client.post = AsyncMock()
    msg = make_message()

    await process_payment(data, msg, http_client)

    msg.reject.assert_awaited_once_with(requeue=False)
    msg.ack.assert_not_awaited()
    msg.nack.assert_not_awaited()
    db_mock.assert_not_awaited()
    http_client.post.assert_not_awaited()
    sleep_mock.assert_not_awaited()
