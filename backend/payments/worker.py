# faststream run backend.payments.worker:app


import asyncio
import logging
import random
from contextlib import asynccontextmanager
from typing import Annotated
from uuid import UUID

import httpx
from faststream import AckPolicy, Context, ContextRepo, FastStream
from faststream.rabbit import Channel, RabbitMessage

from backend.core.broker import (
    broker,
    declare_topology,
    payment_exchange,
    payment_queue,
)
from backend.core.metrics import metrics_server, worker_registry
from backend.db.session import async_session_factory
from backend.payments.retry import handle_processing_failure
from backend.payments.service import (
    complete_payment,
    get_payment_processing_state,
    mark_webhook_delivered,
)
from backend.webhooks.service import deliver_webhook, settings
from backend.webhooks.transport import webhook_client

logger = logging.getLogger(__name__)


@asynccontextmanager
async def lifespan(context: ContextRepo):
    settings.signing_key()
    with metrics_server(settings, worker_registry, settings.worker_metrics_port):
        async with webhook_client() as client:
            context.set_global("http_client", client)
            yield


app = FastStream(broker, lifespan=lifespan)


@broker.subscriber(
    queue=payment_queue,
    exchange=payment_exchange,
    channel=Channel(prefetch_count=1),
    ack_policy=AckPolicy.MANUAL,
    no_reply=True,
)
async def process_payment(
    data: dict,
    msg: RabbitMessage,
    http_client: Annotated[httpx.AsyncClient, Context()],
) -> None:
    try:
        if not isinstance(data, dict) or not isinstance(data.get("payment_id"), str):
            raise TypeError("Expected a string payment_id")
        payment_id = UUID(data["payment_id"])
    except (KeyError, TypeError, ValueError):
        logger.warning("Invalid payment message")
        await msg.reject(requeue=False)
        return

    try:
        is_pending, is_delivered = await get_payment_state_in_db(payment_id)

        if not is_delivered:
            if is_pending:
                await asyncio.sleep(random.uniform(2, 5))

            result = await process_payment_in_db(payment_id)

            if result is not None:
                webhook_url, payload = result

                await deliver_webhook(
                    http_client,
                    webhook_url,
                    payload,
                )

                await mark_webhook_delivered_in_db(payment_id)

    except Exception as exc:  # noqa: BLE001 - manual ACK requires an outcome for every failure
        logger.warning("Payment processing failed: %s", type(exc).__name__)
        await handle_processing_failure(data, msg, exc)
    else:
        await msg.ack()


async def process_payment_in_db(
    payment_id: UUID,
) -> tuple[str, dict] | None:
    async with async_session_factory() as session:
        return await complete_payment(session, payment_id)


@app.on_startup
async def setup_queues() -> None:
    await broker.connect()
    await declare_topology(broker)


async def get_payment_state_in_db(
    payment_id: UUID,
) -> tuple[bool, bool]:
    async with async_session_factory() as session:
        return await get_payment_processing_state(
            session,
            payment_id,
        )


async def mark_webhook_delivered_in_db(
    payment_id: UUID,
) -> None:
    async with async_session_factory() as session:
        await mark_webhook_delivered(session, payment_id)
