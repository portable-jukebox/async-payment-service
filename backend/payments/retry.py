import logging
import random

from faststream.rabbit import RabbitMessage

from backend.core.broker import (
    RETRY_QUEUES,
    broker,
    payment_retry_exchange,
)
from backend.payments.error_policy import (
    get_retry_counter_key,
    is_retryable_error,
)

logger = logging.getLogger(__name__)

MAX_ATTEMPTS = 3

RETRY_COUNTER_KEYS = (
    "db_retry_count",
    "webhook_retry_count",
)


async def handle_processing_failure(
    data: dict,
    msg: RabbitMessage,
    error: Exception,
) -> None:
    if not is_retryable_error(error):
        logger.error(
            "Non-retryable payment processing error: %r",
            type(error).__name__,
        )
        await msg.reject(requeue=False)
        return

    headers = msg.headers or {}

    counters = {key: headers.get(key, 0) for key in RETRY_COUNTER_KEYS}

    if any(
        type(count) is not int or not 0 <= count < MAX_ATTEMPTS
        for count in counters.values()
    ):
        logger.error("Invalid retry counters: %r", counters)
        await msg.reject(requeue=False)
        return

    counter_key = get_retry_counter_key(error)
    retry_count = counters[counter_key]
    attempt = retry_count + 1

    if attempt >= MAX_ATTEMPTS:
        logger.error(
            "%s failed after %s attempts: %r",
            counter_key,
            attempt,
            type(error).__name__,
        )
        await msg.reject(requeue=False)
        return

    retry_queue = random.choice(RETRY_QUEUES[retry_count])

    counters[counter_key] = attempt

    try:
        await broker.publish(
            message=data,
            exchange=payment_retry_exchange,
            routing_key=retry_queue.routing_key,
            headers=counters,
            persist=True,
            mandatory=True,
            timeout=5.0,
        )
    except Exception:
        logger.exception("Failed to publish retry message")
        await msg.nack(requeue=True)
        return

    logger.warning(
        "%s scheduled for retry %s/%s: %r",
        counter_key,
        attempt + 1,
        MAX_ATTEMPTS,
        type(error).__name__,
    )
    await msg.ack()
