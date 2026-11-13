"""Real PostgreSQL, AMQP, TLS and HTTP; only the test wire and faults are injected."""

import asyncio
import hashlib
import hmac
import json
import ssl
import time
from uuid import UUID

import aio_pika
import httpcore
import pytest
from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import async_sessionmaker

from backend.core.config import Settings
from backend.outbox import publisher
from backend.outbox.models import Outbox
from backend.outbox.repository import OutboxRepository
from backend.payments import worker
from backend.payments.models import Payment
from backend.tests.test_broker_integration import eventually
from backend.tests.test_payment_creation import PAYLOAD, payment_client
from backend.tests.test_webhook_security import dns, tls_files

# Re-export shared pytest fixtures for discovery in this module.
__all__ = ["payment_client", "tls_files"]


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "scenario",
    [
        "success",
        "http_recovery",
        "http_exhausted",
        "http_permanent",
        "delivery_commit_gap",
        "publish_commit_gap",
    ],
)
async def test_end_to_end(
    scenario, worker_rabbitmq_url, test_engine, payment_client, tls_files, monkeypatch
):
    factory = async_sessionmaker(test_engine, expire_on_commit=False)
    requests = []
    tls_names = []
    receiver_errors = []
    key, cert = tls_files
    server_context = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
    server_context.load_cert_chain(cert, key)
    server_context.set_servername_callback(
        lambda socket, name, ctx: tls_names.append(name)
    )

    async def receive(reader, writer):
        try:
            raw_headers = await reader.readuntil(b"\r\n\r\n")
            lines = raw_headers.decode("ascii").split("\r\n")
            headers = dict(line.lower().split(": ", 1) for line in lines[1:] if line)
            body = await reader.readexactly(int(headers["content-length"]))
            requests.append((headers, body))
            status = 200
            if scenario == "http_exhausted" or (
                scenario == "http_recovery" and len(requests) < 3
            ):
                status = 503
            elif scenario == "http_permanent":
                status = 400
            writer.write(
                f"HTTP/1.1 {status} Test\r\nContent-Length: 0\r\nConnection: close\r\n\r\n".encode()
            )
            await writer.drain()
        except Exception as exc:  # noqa: BLE001 - surface async server failures in the test
            receiver_errors.append(exc)
        finally:
            writer.close()
            await writer.wait_closed()

    server = await asyncio.start_server(receive, "127.0.0.1", 0, ssl=server_context)
    port = server.sockets[0].getsockname()[1]
    original_connect = httpcore.AnyIOBackend.connect_tcp
    original_resolve = asyncio.get_running_loop().getaddrinfo
    original_context = ssl.create_default_context

    async def resolve(host, requested_port, **kwargs):
        if host == "example.com":
            return dns("93.184.216.34")
        return await original_resolve(host, requested_port, **kwargs)

    async def wire(self, host, requested_port, **kwargs):
        # No security bypass in production. Only after the real policy accepts this
        # numeric public IP does the fixture route the wire to its owned TLS server.
        assert host == "93.184.216.34"
        return await original_connect(self, "127.0.0.1", port, **kwargs)

    def trusted_context():
        context = original_context()
        context.load_verify_locations(cafile=cert)
        return context

    monkeypatch.setattr(asyncio.get_running_loop(), "getaddrinfo", resolve)
    monkeypatch.setattr(httpcore.AnyIOBackend, "connect_tcp", wire)
    monkeypatch.setattr(
        "backend.webhooks.transport.ssl.create_default_context", trusted_context
    )
    monkeypatch.setattr(worker, "async_session_factory", factory)
    monkeypatch.setattr(worker.random, "uniform", lambda a, b: 0)
    monkeypatch.setitem(worker.broker._connection_kwargs, "url", worker_rabbitmq_url)

    delivery_marks = 0
    original_mark = worker.mark_webhook_delivered_in_db

    async def mark_delivery(payment_id):
        nonlocal delivery_marks
        delivery_marks += 1
        if scenario == "delivery_commit_gap" and delivery_marks == 1:
            raise ConnectionError("Injected failure after remote acceptance")
        await original_mark(payment_id)

    monkeypatch.setattr(worker, "mark_webhook_delivered_in_db", mark_delivery)

    async with server, worker.app.lifespan_context():
        try:
            await worker.app.start()
            responses = await asyncio.gather(
                *[
                    payment_client.post(
                        "/api/v1/payments",
                        json=PAYLOAD,
                        headers={"Idempotency-Key": "e2e-key"},
                    )
                    for _ in range(6)
                ]
            )
            assert all(response.status_code == 202 for response in responses)
            assert len({r.json()["payment_id"] for r in responses}) == 1
            payment_id = UUID(responses[0].json()["payment_id"])
            async with factory() as session:
                assert (
                    await session.scalar(select(func.count()).select_from(Payment)) == 1
                )
                assert (
                    await session.scalar(select(func.count()).select_from(Outbox)) == 1
                )
            if scenario == "publish_commit_gap":
                original_mark_published = OutboxRepository.mark_published

                def fail_mark(self, event):
                    raise ConnectionError("Injected failure after broker confirm")

                monkeypatch.setattr(OutboxRepository, "mark_published", fail_mark)
                async with factory() as session:
                    with pytest.raises(ConnectionError):
                        await publisher.publish_batch(session)
                monkeypatch.setattr(
                    OutboxRepository, "mark_published", original_mark_published
                )

                async def already_delivered():
                    async with factory() as session:
                        return (
                            await session.get(Payment, payment_id)
                        ).webhook_delivered_at

                await eventually(already_delivered)
            async with factory() as session:
                assert await publisher.publish_batch(session) == 1

            async with await aio_pika.connect(worker_rabbitmq_url) as connection:
                channel = await connection.channel()
                dlq = await channel.declare_queue("webhook_dlq", passive=True)
                if scenario in {"http_permanent", "http_exhausted"}:
                    dead = await eventually(lambda: dlq.get(fail=False), timeout=15)
                    assert json.loads(dead.body)["payment_id"] == str(payment_id)
                    if scenario == "http_exhausted":
                        assert dead.headers["webhook_retry_count"] == 2
                    await dead.ack()
                else:

                    async def delivered():
                        async with factory() as session:
                            return (
                                await session.get(Payment, payment_id)
                            ).webhook_delivered_at

                    await eventually(delivered, timeout=15)
                    # A marker on the same queue proves the prior duplicate was handled.
                    exchange = await channel.get_exchange("payments")
                    await exchange.publish(
                        aio_pika.Message(b"{marker", content_type="application/json"),
                        routing_key="payment.created",
                    )
                    marker = await eventually(lambda: dlq.get(fail=False))
                    assert marker.body == b"{marker"
                    await marker.ack()
                assert await dlq.get(fail=False) is None

            async with factory() as session:
                payment = await session.get(Payment, payment_id)
                assert payment.status in {"succeeded", "failed"}
                assert payment.processed_at is not None
                assert (payment.webhook_delivered_at is None) == (
                    scenario in {"http_permanent", "http_exhausted"}
                )
                assert await session.scalar(select(Outbox.published_at)) is not None
            expected_count = {
                "http_recovery": 3,
                "http_exhausted": 3,
                "delivery_commit_gap": 2,
            }.get(scenario, 1)
            assert len(requests) == expected_count
            assert tls_names == ["example.com"] * expected_count
            assert not receiver_errors
            for headers, body in requests:
                assert headers["host"] == "example.com"
                assert headers["idempotency-key"] == str(payment_id)
                assert json.loads(body) == {
                    "payment_id": str(payment_id),
                    "status": payment.status,
                }
                assert abs(time.time() - int(headers["x-webhook-timestamp"])) < 30
                expected = hmac.new(
                    Settings().signing_key(),
                    headers["x-webhook-timestamp"].encode() + b"." + body,
                    hashlib.sha256,
                ).hexdigest()
                assert hmac.compare_digest(
                    headers["x-webhook-signature"], "v1=" + expected
                )
        finally:
            await worker.app.stop()
