# python -m backend.outbox.publisher


import asyncio
import logging
import time

from sqlalchemy.ext.asyncio import AsyncSession

from backend.core.broker import (
    broker,
    declare_topology,
    payment_exchange,
)
from backend.core.config import Settings
from backend.core.metrics import (
    metrics_server,
    publish_errors,
    published,
    publisher_registry,
    publisher_success,
)
from backend.db.session import async_session_factory
from backend.outbox.repository import OutboxRepository

logger = logging.getLogger(__name__)


async def publish_batch(session: AsyncSession) -> int:
    repository = OutboxRepository(session)

    async with session.begin():
        events = await repository.get_pending_for_publication()

        for event in events:
            await broker.publish(
                message=event.payload,
                exchange=payment_exchange,
                routing_key=event.event_type,
                persist=True,
                mandatory=True,
                timeout=5.0,
            )

            repository.mark_published(event)

    published.inc(len(events))
    return len(events)


async def run_publisher():
    async with broker:
        await declare_topology(broker)

        while True:
            async with async_session_factory() as session:
                try:
                    started = time.monotonic()
                    count = await publish_batch(session)
                    publisher_success.set_to_current_time()
                    log = logger.info if count else logger.debug
                    log(
                        "Outbox published count=%s duration_seconds=%.3f",
                        count,
                        time.monotonic() - started,
                    )
                except Exception:
                    publish_errors.inc()
                    logger.exception("Outbox publication failed")

            await asyncio.sleep(1)


async def main():
    settings = Settings()
    with metrics_server(settings, publisher_registry, settings.publisher_metrics_port):
        await run_publisher()


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)

    asyncio.run(main())
