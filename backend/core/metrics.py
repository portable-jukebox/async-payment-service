"""Process counters are observations; PostgreSQL gauges are durable state."""

import asyncio
import logging
import time
from contextlib import contextmanager
from datetime import UTC, datetime

from prometheus_client import (
    CollectorRegistry,
    Counter,
    Gauge,
    generate_latest,
    start_http_server,
)
from sqlalchemy import func, select
from sqlalchemy.exc import SQLAlchemyError

from backend.outbox.models import Outbox
from backend.payments.models import Payment

logger = logging.getLogger(__name__)
worker_registry = CollectorRegistry()
publisher_registry = CollectorRegistry()
completed = Counter(
    "payments_completed",
    "Observed committed payment transitions",
    ["status"],
    registry=worker_registry,
)
webhooks = Counter(
    "webhook_requests", "Webhook attempts", ["result"], registry=worker_registry
)
published = Counter(
    "outbox_published",
    "Observed committed publications (duplicates possible)",
    registry=publisher_registry,
)
publish_errors = Counter(
    "outbox_publish_errors", "Failed publisher cycles", registry=publisher_registry
)
publisher_success = Gauge(
    "publisher_last_success_timestamp_seconds",
    "Last completed cycle, including empty cycles",
    registry=publisher_registry,
)

for registry in (worker_registry, publisher_registry):
    Gauge(
        "service_start_timestamp_seconds",
        "Process metrics initialization time",
        registry=registry,
    ).set(time.time())


@contextmanager
def metrics_server(settings, registry, port):
    if not settings.metrics_enabled:
        yield
        return
    server, thread = start_http_server(port, addr="0.0.0.0", registry=registry)
    try:
        yield
    finally:
        server.shutdown()
        server.server_close()
        thread.join(timeout=2)


async def database_metrics(session_factory) -> bytes:
    registry = CollectorRegistry()
    success = Gauge(
        "database_metrics_collection_success",
        "Whether database collection succeeded",
        registry=registry,
    )
    try:
        async with asyncio.timeout(5), session_factory() as session:
            counts = (
                await session.execute(
                    select(Payment.status, func.count()).group_by(Payment.status)
                )
            ).all()
            pending = Outbox.published_at.is_(None)
            count = await session.scalar(
                select(func.count()).select_from(Outbox).where(pending)
            )
            oldest = await session.scalar(
                select(Outbox.created_at)
                .where(pending)
                .order_by(Outbox.created_at)
                .limit(1)
            )
    except (SQLAlchemyError, OSError, TimeoutError):
        logger.warning("Database metrics collection failed")
        success.set(0)
    else:
        payments = Gauge(
            "payments_stored",
            "Current payment rows in PostgreSQL",
            ["status"],
            registry=registry,
        )
        values = dict(counts)
        for status in ("pending", "succeeded", "failed"):
            payments.labels(status).set(values.get(status, 0))
        Gauge(
            "outbox_pending",
            "Current unpublished rows in PostgreSQL",
            registry=registry,
        ).set(count)
        age = max(0, (datetime.now(UTC) - oldest).total_seconds()) if oldest else 0
        Gauge(
            "outbox_oldest_pending_age_seconds",
            "Age of oldest unpublished row",
            registry=registry,
        ).set(age)
        success.set(1)
    return generate_latest(registry)
