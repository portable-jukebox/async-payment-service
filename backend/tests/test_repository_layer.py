import asyncio
from datetime import UTC, datetime, timedelta
from unittest.mock import Mock
from uuid import uuid4

import pytest
from sqlalchemy import select
from sqlalchemy.ext.asyncio import async_sessionmaker

from backend.outbox.models import Outbox
from backend.outbox.repository import OutboxRepository
from backend.payments.models import Payment
from backend.payments.repository import PaymentRepository
from backend.payments.schemas import PaymentCreate
from backend.payments.service import complete_payment, create_payment


def make_payment() -> Payment:
    return Payment(
        amount="100.00",
        currency="RUB",
        description="Repository layer test",
        _metadata={"source": "pytest"},
        webhook_url="https://example.com/webhook",
        idempotency_key=f"payment-{uuid4()}",
    )


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("random_value", "expected_status"),
    [(0.5, "succeeded"), (0.95, "failed")],
)
async def test_complete_payment_is_idempotent(
    test_session, monkeypatch, random_value, expected_status
):
    payment = make_payment()
    test_session.add(payment)
    await test_session.commit()

    random_mock = Mock(return_value=random_value)
    monkeypatch.setattr("backend.payments.service.random.random", random_mock)

    first_url, first_payload = await complete_payment(test_session, payment.id)
    processed_at = payment.processed_at

    assert first_url == payment.webhook_url
    assert first_payload == {
        "payment_id": str(payment.id),
        "status": expected_status,
    }
    assert payment.status == expected_status
    assert processed_at is not None

    second_url, second_payload = await complete_payment(test_session, payment.id)

    assert (second_url, second_payload) == (first_url, first_payload)
    assert payment.status == expected_status
    assert payment.processed_at == processed_at
    random_mock.assert_called_once()


@pytest.mark.asyncio
async def test_complete_payment_missing_payment(test_session):
    missing_id = uuid4()
    with pytest.raises(ValueError, match=str(missing_id)):
        await complete_payment(test_session, missing_id)


@pytest.mark.asyncio
async def test_create_payment_rolls_back_if_outbox_add_fails(test_session, monkeypatch):
    key = f"rollback-{uuid4()}"
    data = PaymentCreate(
        amount="100.00",
        currency="RUB",
        description="Rollback test",
        metadata={"source": "pytest"},
        webhook_url="https://example.com/webhook",
    )

    def fail_outbox_add(self, event_type, payload):
        raise RuntimeError("Outbox insert failed")

    monkeypatch.setattr(OutboxRepository, "add", fail_outbox_add)

    with pytest.raises(RuntimeError, match="Outbox insert failed"):
        await create_payment(test_session, data, key)

    payment_id = await test_session.scalar(
        select(Payment.id).where(Payment.idempotency_key == key)
    )
    outbox_id = await test_session.scalar(select(Outbox.id))

    assert payment_id is None
    assert outbox_id is None


@pytest.mark.asyncio
async def test_concurrent_complete_payment_processes_once(
    test_session, test_engine, monkeypatch
):
    payment = make_payment()
    test_session.add(payment)
    await test_session.commit()

    random_mock = Mock(return_value=0.5)
    monkeypatch.setattr("backend.payments.service.random.random", random_mock)

    original_get = PaymentRepository.get_by_id_for_update
    first_locked = asyncio.Event()
    second_started = asyncio.Event()
    release_first = asyncio.Event()
    call_count = 0

    async def controlled_get(self, payment_id):
        nonlocal call_count
        call_count += 1
        if call_count == 1:
            row = await original_get(self, payment_id)
            first_locked.set()
            await release_first.wait()
            return row
        second_started.set()
        return await original_get(self, payment_id)

    monkeypatch.setattr(PaymentRepository, "get_by_id_for_update", controlled_get)
    session_factory = async_sessionmaker(test_engine, expire_on_commit=False)

    async def run_once():
        async with session_factory() as session:
            return await complete_payment(session, payment.id)

    task1 = asyncio.create_task(run_once())
    task2 = None
    try:
        await asyncio.wait_for(first_locked.wait(), timeout=5)
        task2 = asyncio.create_task(run_once())
        await asyncio.wait_for(second_started.wait(), timeout=5)
        assert not task2.done(), "Second transaction did not wait for row lock"
    finally:
        release_first.set()
        tasks = [task1] if task2 is None else [task1, task2]
        results = await asyncio.wait_for(asyncio.gather(*tasks), timeout=10)

    assert task2 is not None
    assert results[0] == results[1]
    random_mock.assert_called_once()

    # Use a separate, short-lived session to check the persisted result.
    async with session_factory() as session:
        persisted = await session.get(Payment, payment.id)
        assert persisted is not None
        assert persisted.status == "succeeded"
        assert persisted.processed_at is not None


@pytest.mark.asyncio
async def test_outbox_repository_skips_locked_rows(test_session, test_engine):
    now = datetime.now(UTC)
    first = Outbox(
        event_type="payment.created",
        payload={"payment_id": "first"},
        created_at=now,
    )
    second = Outbox(
        event_type="payment.created",
        payload={"payment_id": "second"},
        created_at=now + timedelta(seconds=1),
    )
    test_session.add_all([first, second])
    await test_session.commit()

    session_factory = async_sessionmaker(test_engine, expire_on_commit=False)
    async with (
        session_factory() as session1,
        session_factory() as session2,
        session1.begin(),
    ):
        locked = await OutboxRepository(session1).get_pending_for_publication(limit=1)
        assert [event.id for event in locked] == [first.id]

        async with session2.begin():
            available = await asyncio.wait_for(
                OutboxRepository(session2).get_pending_for_publication(),
                timeout=5,
            )
            assert [event.id for event in available] == [second.id]
