from datetime import UTC, datetime, timedelta
from unittest.mock import AsyncMock

import pytest
from sqlalchemy import func, inspect, select, text
from sqlalchemy.ext.asyncio import async_sessionmaker

from backend.core.config import Settings
from backend.outbox import cleanup, publisher
from backend.outbox.models import Outbox
from backend.outbox.repository import OutboxRepository
from backend.payments.models import Payment


@pytest.mark.asyncio
async def test_cleanup_dry_run_retention_and_payment_preservation(test_session):
    now = datetime.now(UTC)
    old = now - timedelta(days=10)
    entries = [
        Outbox(
            event_type="payment.created", payload={}, created_at=old, published_at=old
        ),
        Outbox(
            event_type="payment.created", payload={}, created_at=old, published_at=now
        ),
        Outbox(event_type="payment.created", payload={}, created_at=old),
    ]
    test_session.add_all(entries)
    test_session.add(
        Payment(
            amount="1",
            currency="RUB",
            description="retained",
            _metadata={},
            webhook_url="https://example.com",
            idempotency_key="retained",
        )
    )
    await test_session.commit()
    retained_ids = {entries[1].id, entries[2].id}
    cutoff = now - timedelta(days=7)
    async with test_session.begin():
        assert await OutboxRepository(test_session).cleanup_published(cutoff) == 1
    assert await test_session.scalar(select(func.count()).select_from(Outbox)) == 3
    await test_session.rollback()
    async with test_session.begin():
        assert (
            await OutboxRepository(test_session).cleanup_published(
                cutoff, dry_run=False
            )
            == 1
        )
    assert set(await test_session.scalars(select(Outbox.id))) == retained_ids
    assert await test_session.scalar(select(func.count()).select_from(Payment)) == 1


@pytest.mark.asyncio
async def test_concurrent_cleanup_batches_skip_locked(test_engine, test_session):
    old = datetime.now(UTC) - timedelta(days=10)
    test_session.add_all(
        [Outbox(event_type="test", payload={}, published_at=old) for _ in range(10)]
    )
    await test_session.commit()
    factory = async_sessionmaker(test_engine)
    async with factory() as first, factory() as second, first.begin(), second.begin():
        assert (
            await OutboxRepository(first).cleanup_published(
                datetime.now(UTC), 5, dry_run=False
            )
            == 5
        )
        assert (
            await OutboxRepository(second).cleanup_published(
                datetime.now(UTC), 5, dry_run=False
            )
            == 5
        )
    assert await test_session.scalar(select(func.count()).select_from(Outbox)) == 0


@pytest.mark.asyncio
async def test_cleanup_requires_explicit_opt_in(monkeypatch):
    factory = AsyncMock()
    monkeypatch.setattr(cleanup, "async_session_factory", factory)
    with pytest.raises(ValueError, match="OUTBOX_CLEANUP_ENABLED"):
        await cleanup.run_cleanup(Settings(outbox_cleanup_enabled=False), execute=True)
    factory.assert_not_called()


@pytest.mark.asyncio
async def test_cleanup_default_is_read_only(test_engine, test_session, monkeypatch):
    old = datetime.now(UTC) - timedelta(days=30)
    test_session.add(Outbox(event_type="test", payload={}, published_at=old))
    await test_session.commit()
    monkeypatch.setattr(
        cleanup, "async_session_factory", async_sessionmaker(test_engine)
    )
    assert await cleanup.run_cleanup(Settings(outbox_cleanup_enabled=True)) == 1
    assert await test_session.scalar(select(func.count()).select_from(Outbox)) == 1


@pytest.mark.asyncio
async def test_partial_publication_rolls_back_marks_and_republishes(
    test_session, monkeypatch
):
    events = [Outbox(event_type="payment.created", payload={"n": n}) for n in range(2)]
    test_session.add_all(events)
    await test_session.commit()
    publish = AsyncMock(side_effect=[None, ConnectionError("test broker outage")])
    monkeypatch.setattr(publisher.broker, "publish", publish)
    with pytest.raises(ConnectionError):
        await publisher.publish_batch(test_session)
    assert list(await test_session.scalars(select(Outbox.published_at))) == [None, None]
    await test_session.rollback()
    publish.side_effect = None
    assert await publisher.publish_batch(test_session) == 2
    assert publish.await_count == 4


@pytest.mark.asyncio
async def test_pending_partial_index_is_used_on_large_history(test_engine):
    async with test_engine.begin() as connection:
        await connection.execute(
            text("""
            INSERT INTO outbox (id, event_type, payload, published_at)
            SELECT gen_random_uuid(), 'payment.created', '{}', now() FROM generate_series(1, 10000)
        """)
        )
        await connection.execute(
            text(
                "INSERT INTO outbox (id, event_type, payload) VALUES (gen_random_uuid(), 'payment.created', '{}')"
            )
        )
        await connection.execute(text("ANALYZE outbox"))
        plan = await connection.scalar(
            text("""
            EXPLAIN (FORMAT JSON) SELECT * FROM outbox
            WHERE published_at IS NULL AND event_type = 'payment.created'
            ORDER BY created_at, id LIMIT 50 FOR UPDATE SKIP LOCKED
        """)
        )
        assert "ix_outbox_pending" in str(plan)
        indexes = await connection.run_sync(
            lambda conn: inspect(conn).get_indexes("outbox")
        )
        assert {index["name"] for index in indexes} == {
            "ix_outbox_pending",
            "ix_outbox_published_cleanup",
        }
