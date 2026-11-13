import asyncio
from decimal import Decimal
from unittest.mock import AsyncMock

import pytest
import pytest_asyncio
from httpx import ASGITransport, AsyncClient
from sqlalchemy import func, select, text
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import async_sessionmaker

from backend.core import security
from backend.db.session import get_async_session
from backend.main import app
from backend.outbox.models import Outbox
from backend.outbox.repository import OutboxRepository
from backend.payments.models import Payment
from backend.payments.repository import PaymentRepository
from backend.payments.schemas import PaymentCreate
from backend.payments.service import create_payment

PAYLOAD = {
    "amount": "100.00",
    "currency": "RUB",
    "description": "Stage 1 payment",
    "metadata": {"order": 1},
    "webhook_url": "https://example.com/webhook",
}


@pytest_asyncio.fixture
async def payment_client(test_engine, monkeypatch):
    factory = async_sessionmaker(test_engine, expire_on_commit=False)

    async def provide_session():
        async with factory() as session:
            yield session

    monkeypatch.setitem(app.dependency_overrides, get_async_session, provide_session)
    async with AsyncClient(
        transport=ASGITransport(app=app, raise_app_exceptions=False),
        base_url="http://test",
        headers={"X-API-Key": security.settings.api_key},
    ) as client:
        yield client


@pytest.mark.asyncio
@pytest.mark.parametrize("key", [b"", b" ", b"\t \t", b"\xa0"])
async def test_blank_idempotency_key_rejected_before_creation(monkeypatch, key):
    create = AsyncMock()
    monkeypatch.setattr("backend.payments.router.create_payment", create)
    async with AsyncClient(
        transport=ASGITransport(app=app), base_url="http://test"
    ) as client:
        response = await client.post(
            "/api/v1/payments",
            json=PAYLOAD,
            headers=[
                (b"x-api-key", security.settings.api_key.encode("ascii")),
                (b"idempotency-key", key),
            ],
        )
    assert response.status_code == 422
    assert any(
        error["loc"] == ["header", "idempotency-key"]
        for error in response.json()["detail"]
    )
    create.assert_not_awaited()


@pytest.mark.asyncio
@pytest.mark.parametrize("key", ["x", "x" * 255, " order-1 ", "\torder-1\t"])
async def test_nonblank_key_is_preserved(payment_client, test_session, key):
    headers = {"Idempotency-Key": key}
    first = await payment_client.post("/api/v1/payments", json=PAYLOAD, headers=headers)
    second = await payment_client.post(
        "/api/v1/payments", json=PAYLOAD, headers=headers
    )
    assert first.status_code == second.status_code == 202
    assert first.json() == second.json()
    assert await test_session.scalar(select(Payment.idempotency_key)) == key
    assert await test_session.scalar(select(func.count()).select_from(Outbox)) == 1


@pytest.mark.asyncio
async def test_whitespace_does_not_merge_distinct_keys(payment_client, test_session):
    identifiers = set()
    for key in ("order-1", " order-1 "):
        response = await payment_client.post(
            "/api/v1/payments", json=PAYLOAD, headers={"Idempotency-Key": key}
        )
        assert response.status_code == 202
        identifiers.add(response.json()["payment_id"])
    assert len(identifiers) == 2
    assert await test_session.scalar(select(func.count()).select_from(Outbox)) == 2


@pytest.mark.asyncio
@pytest.mark.parametrize("path", ["/health", "/api/v1/payments"])
@pytest.mark.parametrize("key", [b"\xff", "ключ".encode()])
async def test_non_ascii_api_key_returns_401(path, key):
    async with AsyncClient(
        transport=ASGITransport(app=app), base_url="http://test"
    ) as client:
        if path == "/health":
            response = await client.get(path, headers=[(b"x-api-key", key)])
        else:
            response = await client.post(
                path,
                json=PAYLOAD,
                headers=[(b"x-api-key", key), (b"idempotency-key", b"auth-test")],
            )
    assert response.status_code == 401
    assert response.json() == {"detail": "Invalid API key"}


@pytest.mark.asyncio
async def test_non_ascii_configured_api_key_fails_closed(monkeypatch):
    monkeypatch.setattr(security.settings, "api_key", "ключ")
    async with AsyncClient(
        transport=ASGITransport(app=app), base_url="http://test"
    ) as client:
        response = await client.get("/health", headers={"X-API-Key": "ascii-key"})
    assert response.status_code == 401


@pytest.mark.asyncio
@pytest.mark.parametrize("scenario", ["same", "different", "rollback"])
async def test_concurrent_creation_waits_for_database_winner(
    payment_client, test_engine, test_session, monkeypatch, scenario
):
    inserted = asyncio.Event()
    release = asyncio.Event()
    original_insert = PaymentRepository.add_if_absent
    calls = 0

    async def hold_first_insert(self, payment):
        nonlocal calls
        calls += 1
        first = calls == 1
        result = await original_insert(self, payment)
        if first:
            inserted.set()
            await release.wait()
        return result

    monkeypatch.setattr(PaymentRepository, "add_if_absent", hold_first_insert)
    if scenario == "rollback":
        original_add = OutboxRepository.add
        outbox_calls = 0

        def fail_first_outbox(self, event_type, payload):
            nonlocal outbox_calls
            outbox_calls += 1
            return original_add(
                self, None if outbox_calls == 1 else event_type, payload
            )

        monkeypatch.setattr(OutboxRepository, "add", fail_first_outbox)

    async def request(payload):
        return await payment_client.post(
            "/api/v1/payments", json=payload, headers={"Idempotency-Key": "concurrent"}
        )

    first = asyncio.create_task(request(PAYLOAD))
    tasks = [first]
    try:
        await asyncio.wait_for(inserted.wait(), timeout=5)
        second_payload = (
            PAYLOAD if scenario == "same" else {**PAYLOAD, "amount": "200.00"}
        )
        second = asyncio.create_task(request(second_payload))
        tasks.append(second)

        async def wait_for_insert_lock():
            async with test_engine.connect() as connection:
                while True:
                    waiting = await connection.scalar(
                        text(
                            "SELECT EXISTS (SELECT 1 FROM pg_stat_activity "
                            "WHERE datname = current_database() AND wait_event_type = 'Lock' "
                            "AND query LIKE 'INSERT INTO payments%')"
                        )
                    )
                    await connection.commit()
                    if waiting:
                        return
                    await asyncio.sleep(0.01)

        await asyncio.wait_for(wait_for_insert_lock(), timeout=5)
        assert not second.done()
    finally:
        release.set()
        responses = await asyncio.wait_for(asyncio.gather(*tasks), timeout=10)

    expected = {"same": [202, 202], "different": [202, 409], "rollback": [500, 202]}
    assert [response.status_code for response in responses] == expected[scenario]
    if scenario == "same":
        assert responses[0].json() == responses[1].json()
    payments = (await test_session.scalars(select(Payment))).all()
    events = (await test_session.scalars(select(Outbox))).all()
    assert len(payments) == len(events) == 1
    assert payments[0].amount == Decimal("200" if scenario == "rollback" else "100")
    assert events[0].event_type == "payment.created"
    assert events[0].payload == {"payment_id": str(payments[0].id)}


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("changes", "constraint"),
    [
        ({"amount": Decimal(0)}, "ck_payments_amount_positive"),
        ({"amount": Decimal(-1)}, "ck_payments_amount_positive"),
        ({"currency": "GBP"}, "ck_payments_currency"),
        ({"description": None}, "description"),
    ],
)
async def test_other_integrity_errors_propagate_and_rollback(
    test_session, changes, constraint
):
    # Deliberately bypass API validation to exercise the actual INSERT constraints.
    data = PaymentCreate(**PAYLOAD).model_copy(update=changes)
    with pytest.raises(IntegrityError, match=constraint):
        await create_payment(test_session, data, "bad-data")
    assert await test_session.scalar(select(func.count()).select_from(Payment)) == 0
    assert await test_session.scalar(select(func.count()).select_from(Outbox)) == 0


@pytest.mark.asyncio
async def test_unrelated_unique_violation_is_not_idempotency_conflict(test_session):
    await test_session.execute(
        text(
            "ALTER TABLE payments ADD CONSTRAINT test_description_unique UNIQUE (description)"
        )
    )
    await test_session.commit()
    data = PaymentCreate(**PAYLOAD)
    await create_payment(test_session, data, "first-key")
    with pytest.raises(IntegrityError, match="test_description_unique"):
        await create_payment(test_session, data, "different-key")
    assert await test_session.scalar(select(func.count()).select_from(Payment)) == 1
    assert await test_session.scalar(select(func.count()).select_from(Outbox)) == 1


@pytest.mark.asyncio
async def test_database_outbox_failure_rolls_back_and_allows_retry(
    test_session, monkeypatch
):
    data = PaymentCreate(**PAYLOAD)
    original_add = OutboxRepository.add
    with monkeypatch.context() as patch:
        patch.setattr(
            OutboxRepository,
            "add",
            lambda self, event_type, payload: original_add(self, None, payload),
        )
        with pytest.raises(IntegrityError, match="event_type"):
            await create_payment(test_session, data, "retry-after-rollback")
    assert await test_session.scalar(select(func.count()).select_from(Payment)) == 0
    assert await test_session.scalar(select(func.count()).select_from(Outbox)) == 0
    await test_session.rollback()
    payment = await create_payment(test_session, data, "retry-after-rollback")
    assert await test_session.scalar(select(func.count()).select_from(Payment)) == 1
    assert await test_session.scalar(select(Outbox.payload)) == {
        "payment_id": str(payment.id)
    }
