import asyncio
import json
import time
from unittest.mock import AsyncMock
from uuid import uuid4

import aio_pika
import httpx
import pytest
from sqlalchemy.ext.asyncio import async_sessionmaker

from backend.core.broker import RETRY_QUEUES
from backend.payments import worker
from backend.payments.models import Payment


async def eventually(action, timeout=12):
    async with asyncio.timeout(timeout):
        while True:
            value = await action()
            if value:
                return value
            await asyncio.sleep(0.03)


@pytest.mark.asyncio
async def test_topology_before_consumer_and_poison_messages_do_not_block(
    worker_rabbitmq_url, test_engine, test_session, monkeypatch
):
    payment = Payment(
        amount="1",
        currency="RUB",
        description="test",
        _metadata={},
        webhook_url="https://example.com/hook",
        idempotency_key="live",
    )
    test_session.add(payment)
    await test_session.commit()
    monkeypatch.setattr(
        worker,
        "async_session_factory",
        async_sessionmaker(test_engine, expire_on_commit=False),
    )
    monkeypatch.setattr(worker.random, "uniform", lambda a, b: 0)
    http_requests = []

    def receiver(request):
        http_requests.append(request)
        return httpx.Response(200)

    monkeypatch.setattr(
        worker,
        "webhook_client",
        lambda: httpx.AsyncClient(transport=httpx.MockTransport(receiver)),
    )
    original_topology = worker.declare_topology
    checked = []
    async with await aio_pika.connect(worker_rabbitmq_url) as observer:
        channel = await observer.channel()

        async def verify_topology(broker):
            await original_topology(broker)
            for name in [
                "payments.new",
                "webhook_dlq",
                *(q.name for group in RETRY_QUEUES for q in group),
            ]:
                queue = await channel.declare_queue(name, passive=True)
                assert queue.declaration_result.consumer_count == 0
                checked.append(name)

        monkeypatch.setattr(worker, "declare_topology", verify_topology)
        monkeypatch.setitem(
            worker.broker._connection_kwargs, "url", worker_rabbitmq_url
        )
        async with worker.app.lifespan_context():
            try:
                await worker.app.start()
                assert len(checked) == 8
                exchange = await channel.get_exchange("payments")
                invalid = [
                    b"{broken",
                    b"[]",
                    b"null",
                    b"123",
                    b'{"payment_id":123}',
                    b'{"payment_id":{}}',
                ]
                for body in invalid:
                    await exchange.publish(
                        aio_pika.Message(body, content_type="application/json"),
                        routing_key="payment.created",
                    )
                await exchange.publish(
                    aio_pika.Message(
                        json.dumps({"payment_id": str(payment.id)}).encode(),
                        content_type="application/json",
                    ),
                    routing_key="payment.created",
                )

                async def delivered():
                    await test_session.refresh(payment)
                    return payment.webhook_delivered_at

                await eventually(delivered)
                assert len(http_requests) == 1
                assert "X-Webhook-Signature" in http_requests[0].headers
                dlq = await channel.declare_queue("webhook_dlq", passive=True)
                for _ in invalid:
                    message = await eventually(lambda: dlq.get(fail=False))
                    await message.ack()
                assert await dlq.get(fail=False) is None
            finally:
                await worker.app.stop()


@pytest.mark.asyncio
async def test_real_retry_ttl_and_dlq(worker_rabbitmq_url, monkeypatch):
    # Processing stage fails, while delivery, TTL, requeue and DLX remain real.
    attempts = []

    async def fail(payment_id):
        attempts.append(time.monotonic())
        raise httpx.ConnectError("synthetic endpoint outage")

    monkeypatch.setattr(
        worker, "get_payment_state_in_db", AsyncMock(return_value=(False, False))
    )
    monkeypatch.setattr(worker, "process_payment_in_db", fail)
    monkeypatch.setitem(worker.broker._connection_kwargs, "url", worker_rabbitmq_url)
    async with worker.app.lifespan_context():
        try:
            await worker.app.start()
            async with await aio_pika.connect(worker_rabbitmq_url) as observer:
                channel = await observer.channel()
                exchange = await channel.get_exchange("payments")
                await exchange.publish(
                    aio_pika.Message(
                        json.dumps({"payment_id": str(uuid4())}).encode(),
                        content_type="application/json",
                    ),
                    routing_key="payment.created",
                )
                dlq = await channel.declare_queue("webhook_dlq", passive=True)
                message = await eventually(lambda: dlq.get(fail=False), timeout=15)
                assert len(attempts) == 3
                assert attempts[1] - attempts[0] >= 0.9
                assert attempts[2] - attempts[1] >= 1.9
                assert message.headers["webhook_retry_count"] == 2
                assert message.headers["db_retry_count"] == 0
                await message.ack()
        finally:
            await worker.app.stop()
