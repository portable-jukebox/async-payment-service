from datetime import UTC, datetime, timedelta
from unittest.mock import AsyncMock

import httpx
import pytest
from prometheus_client import generate_latest
from prometheus_client.parser import text_string_to_metric_families
from sqlalchemy.ext.asyncio import async_sessionmaker

from backend import main
from backend.core.metrics import database_metrics, publisher_registry, worker_registry
from backend.outbox.models import Outbox
from backend.payments.models import Payment
from backend.payments.service import complete_payment
from backend.webhooks.service import deliver_webhook


def samples(data):
    if isinstance(data, bytes):
        data = data.decode()
    return {
        (s.name, tuple(sorted(s.labels.items()))): s.value
        for family in text_string_to_metric_families(data)
        for s in family.samples
    }


@pytest.mark.asyncio
async def test_database_metrics_are_durable_and_separate(test_engine, monkeypatch):
    factory = async_sessionmaker(test_engine, expire_on_commit=False)
    async with factory() as session, session.begin():
        session.add(
            Payment(
                amount=10,
                description="Metrics test",
                _metadata={},
                currency="USD",
                webhook_url="https://example.com/",
                idempotency_key="metric",
            )
        )
        session.add(
            Outbox(
                event_type="payment.created",
                payload={},
                created_at=datetime.now(UTC) - timedelta(seconds=120),
            )
        )
    monkeypatch.setattr(main, "async_session_factory", factory)
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=main.app), base_url="http://test"
    ) as client:
        first = await client.get("/metrics")
        second = await client.get("/metrics")
    assert first.status_code == second.status_code == 200
    for response in (first, second):
        values = samples(response.text)
        assert values[("database_metrics_collection_success", ())] == 1
        assert values[("payments_stored", (("status", "pending"),))] == 1
        assert values[("outbox_pending", ())] == 1
        assert values[("outbox_oldest_pending_age_seconds", ())] >= 120
        assert "webhook_requests" not in response.text
        assert "outbox_published" not in response.text


@pytest.mark.asyncio
async def test_database_failure_does_not_report_false_zero():
    session = AsyncMock()
    session.__aenter__.side_effect = OSError("database unavailable")
    result = await database_metrics(lambda: session)
    assert samples(result)[("database_metrics_collection_success", ())] == 0
    assert b"outbox_pending" not in result
    assert b"payments_stored" not in result


@pytest.mark.asyncio
async def test_completed_counter_only_observes_new_transitions(test_session):
    payment = Payment(
        amount=10,
        description="Metrics test",
        _metadata={},
        currency="USD",
        webhook_url="https://example.com/",
        idempotency_key="counter",
    )
    test_session.add(payment)
    await test_session.commit()
    before = samples(generate_latest(worker_registry))
    await complete_payment(test_session, payment.id)
    await complete_payment(test_session, payment.id)
    after = samples(generate_latest(worker_registry))
    key = ("payments_completed_total", (("status", payment.status),))
    assert after[key] - before.get(key, 0) == 1
    assert b"payments_completed" not in generate_latest(publisher_registry)


@pytest.mark.asyncio
@pytest.mark.parametrize("status,result", [(200, "success"), (503, "http_error")])
async def test_webhook_counters(status, result):
    key = ("webhook_requests_total", (("result", result),))
    before = samples(generate_latest(worker_registry)).get(key, 0)
    async with httpx.AsyncClient(
        transport=httpx.MockTransport(lambda r: httpx.Response(status))
    ) as client:
        if status == 200:
            await deliver_webhook(
                client, "https://example.com/", {"payment_id": "test"}
            )
        else:
            with pytest.raises(httpx.HTTPStatusError):
                await deliver_webhook(
                    client, "https://example.com/", {"payment_id": "test"}
                )
    assert samples(generate_latest(worker_registry))[key] == before + 1
