import aio_pika
import pytest
from sqlalchemy import text
from sqlalchemy.engine import make_url


@pytest.mark.asyncio
async def test_postgres_uses_owned_migrated_database(test_engine, test_database_url):
    async with test_engine.connect() as connection:
        name = await connection.scalar(text("SELECT current_database()"))
        assert name == make_url(test_database_url).database
        assert name.startswith("pytest_")
        assert await connection.scalar(text("SELECT version_num FROM alembic_version"))


@pytest.mark.asyncio
async def test_rabbitmq_round_trip_on_exclusive_queue(isolated_rabbitmq_url):
    async with await aio_pika.connect(isolated_rabbitmq_url, timeout=5) as connection:
        channel = await connection.channel(
            publisher_confirms=True, on_return_raises=True
        )
        queue = await channel.declare_queue(exclusive=True, auto_delete=True)
        await channel.default_exchange.publish(
            aio_pika.Message(body=b"isolated-infrastructure-check"),
            routing_key=queue.name,
            mandatory=True,
            timeout=5,
        )
        message = await queue.get(timeout=5)
        async with message.process():
            assert message.body == b"isolated-infrastructure-check"
