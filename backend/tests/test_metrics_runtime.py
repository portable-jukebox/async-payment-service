import asyncio
import time

import aio_pika
import httpx
import pytest

from backend.tests.test_compose_runtime import compose, inspect_service
from backend.tests.test_metrics import samples

pytestmark = [pytest.mark.integration, pytest.mark.runtime]


def scrape(port, path="/metrics", expected=None):
    deadline = time.monotonic() + 30
    with httpx.Client(trust_env=False, timeout=3) as client:
        while time.monotonic() < deadline:
            try:
                response = client.get(f"http://127.0.0.1:{port}{path}")
                response.raise_for_status()
                values = samples(response.text)
                if expected is None or expected(values):
                    return values
            except httpx.HTTPError:
                pass
            time.sleep(0.2)
    pytest.fail(f"Metrics endpoint {port} did not become ready")


def test_process_endpoints_and_restart(isolated_postgres, isolated_rabbitmq_url):
    before_db = scrape(58000)
    assert before_db[("database_metrics_collection_success", ())] == 1
    for service, port in (("payment-worker", 59001), ("publisher", 59002)):
        inspect_service(service)
        before = scrape(port)
        start = before[("service_start_timestamp_seconds", ())]
        compose("restart", service)
        after = scrape(
            port,
            expected=lambda v, start=start: (
                v[("service_start_timestamp_seconds", ())] > start
            ),
        )
        assert not any(key[0] == "payments_stored" for key in after)
        if service == "publisher":
            assert after[("outbox_published_total", ())] == 0
            scrape(
                port,
                expected=lambda v, start=start: (
                    v[("publisher_last_success_timestamp_seconds", ())] > start
                ),
            )
        else:
            assert not any(key[0] == "outbox_published_total" for key in after)
    after_db = scrape(58000)
    assert {k: v for k, v in before_db.items() if k[0] == "payments_stored"} == {
        k: v for k, v in after_db.items() if k[0] == "payments_stored"
    }


@pytest.mark.asyncio
async def test_rabbitmq_exporter_reports_actual_dlq(worker_rabbitmq_url):
    vhost = worker_rabbitmq_url.rsplit("/", 1)[-1]
    async with await aio_pika.connect_robust(worker_rabbitmq_url) as connection:
        channel = await connection.channel()
        queue = await channel.declare_queue("webhook_dlq", durable=True)
        await channel.default_exchange.publish(
            aio_pika.Message(b"owned-test-message"), routing_key=queue.name
        )
        key = ("rabbitmq_queue_messages", (("queue", "webhook_dlq"), ("vhost", vhost)))
        await asyncio.to_thread(
            scrape, 55692, "/metrics/per-object", lambda v: v.get(key) == 1
        )
        message = await queue.get(timeout=5)
        await message.ack()
