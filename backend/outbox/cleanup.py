"""Bounded outbox maintenance. Dry-run unless explicitly enabled AND --execute."""

import argparse
import asyncio
import logging
from datetime import UTC, datetime, timedelta

from backend.core.config import Settings
from backend.db.session import async_session_factory
from backend.outbox.repository import OutboxRepository

logger = logging.getLogger(__name__)


async def run_cleanup(settings: Settings, *, execute: bool = False) -> int:
    if execute and not settings.outbox_cleanup_enabled:
        raise ValueError("Execution requires OUTBOX_CLEANUP_ENABLED=true")
    async with async_session_factory() as session, session.begin():
        count = await OutboxRepository(session).cleanup_published(
            datetime.now(UTC) - timedelta(days=settings.outbox_retention_days),
            settings.outbox_cleanup_batch_size,
            dry_run=not execute,
        )
    logger.info("Outbox cleanup dry_run=%s batch_count=%s", not execute, count)
    return count


async def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--execute", action="store_true")
    parser.add_argument("--periodic", action="store_true")
    args = parser.parse_args()
    settings = Settings()
    if args.execute and not settings.outbox_cleanup_enabled:
        parser.error("--execute requires OUTBOX_CLEANUP_ENABLED=true")
    while True:
        await run_cleanup(settings, execute=args.execute)
        if not args.periodic:
            return
        await asyncio.sleep(settings.outbox_cleanup_interval_seconds)


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    asyncio.run(main())
