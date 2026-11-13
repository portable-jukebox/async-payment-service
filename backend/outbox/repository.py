from collections.abc import Sequence
from datetime import UTC, datetime

from sqlalchemy import delete, select
from sqlalchemy.ext.asyncio import AsyncSession

from backend.outbox.models import Outbox


class OutboxRepository:
    def __init__(self, session: AsyncSession):
        self.session = session

    def add(self, event_type: str, payload: dict) -> Outbox:
        event = Outbox(
            event_type=event_type,
            payload=payload,
        )
        self.session.add(event)
        return event

    async def get_pending_for_publication(
        self,
        limit: int = 50,
    ) -> Sequence[Outbox]:
        stmt = (
            select(Outbox)
            .where(
                Outbox.published_at.is_(None),
                Outbox.event_type == "payment.created",
            )
            .order_by(Outbox.created_at, Outbox.id)
            .limit(limit)
            .with_for_update(skip_locked=True)
        )

        result = await self.session.scalars(stmt)
        return result.all()

    def mark_published(self, event: Outbox) -> None:
        event.published_at = datetime.now(UTC)

    async def cleanup_published(
        self, cutoff: datetime, limit: int = 500, *, dry_run: bool = True
    ) -> int:
        if not 1 <= limit <= 10_000:
            raise ValueError("Cleanup batch size must be between 1 and 10000")
        candidates = (
            select(Outbox.id)
            .where(Outbox.published_at.is_not(None), Outbox.published_at < cutoff)
            .order_by(Outbox.published_at, Outbox.id)
            .limit(limit)
            .with_for_update(skip_locked=True)
        )
        if dry_run:
            return len((await self.session.scalars(candidates)).all())
        result = await self.session.scalars(
            delete(Outbox).where(Outbox.id.in_(candidates)).returning(Outbox.id)
        )
        return len(result.all())
