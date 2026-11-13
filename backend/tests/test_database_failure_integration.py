import pytest
from sqlalchemy import text
from sqlalchemy.exc import DBAPIError

from backend.payments.error_policy import get_retry_counter_key, is_retryable_error


@pytest.mark.asyncio
async def test_real_postgres_lock_timeout_is_retryable(test_engine):
    async with (
        test_engine.connect() as holder,
        test_engine.connect() as contender,
        holder.begin(),
    ):
        await holder.execute(text("LOCK TABLE payments IN ACCESS EXCLUSIVE MODE"))
        async with contender.begin():
            await contender.execute(text("SET LOCAL lock_timeout = '100ms'"))
            with pytest.raises(DBAPIError) as caught:
                await contender.execute(text("SELECT count(*) FROM payments"))
            assert caught.value.orig.sqlstate == "55P03"
            assert is_retryable_error(caught.value)
            assert get_retry_counter_key(caught.value) == "db_retry_count"
            await contender.rollback()
