from faststream.rabbit import (
    Channel,
    ExchangeType,
    RabbitBroker,
    RabbitExchange,
    RabbitQueue,
)

from backend.core.config import Settings
from backend.core.middleware import RejectUnhandledMessage

settings = Settings()

broker = RabbitBroker(
    settings.rabbitmq_url,
    middlewares=[RejectUnhandledMessage],
    default_channel=Channel(
        publisher_confirms=True,
        on_return_raises=True,
    ),
)

payment_exchange = RabbitExchange(
    name="payments",
    type=ExchangeType.DIRECT,
    durable=True,
)

webhook_dlx = RabbitExchange(
    name="webhook.dlx",
    type=ExchangeType.DIRECT,
    durable=True,
)

payment_queue = RabbitQueue(
    name="payments.new",
    routing_key="payment.created",
    durable=True,
    arguments={
        "x-dead-letter-exchange": "webhook.dlx",
        "x-dead-letter-routing-key": "webhook.failed",
    },
)

webhook_dlq = RabbitQueue(
    name="webhook_dlq",
    routing_key="webhook.failed",
    durable=True,
)

payment_retry_exchange = RabbitExchange(
    name="payments.retry",
    type=ExchangeType.DIRECT,
    durable=True,
)

payment_retry_queue_1s = RabbitQueue(
    name="payments.retry.1s",
    routing_key="payment.retry.1s",
    durable=True,
    arguments={
        "x-message-ttl": 1_000,
        "x-dead-letter-exchange": "payments",
        "x-dead-letter-routing-key": "payment.created",
    },
)

payment_retry_queue_2s = RabbitQueue(
    name="payments.retry.2s",
    routing_key="payment.retry.2s",
    durable=True,
    arguments={
        "x-message-ttl": 2_000,
        "x-dead-letter-exchange": "payments",
        "x-dead-letter-routing-key": "payment.created",
    },
)


def jitter_queue(delay_ms: int) -> RabbitQueue:
    return RabbitQueue(
        name=f"payments.retry.{delay_ms}ms",
        routing_key=f"payment.retry.{delay_ms}ms",
        durable=True,
        arguments={
            "x-message-ttl": delay_ms,
            "x-dead-letter-exchange": "payments",
            "x-dead-letter-routing-key": "payment.created",
        },
    )


RETRY_QUEUES = (
    (payment_retry_queue_1s, jitter_queue(1250), jitter_queue(1500)),
    (payment_retry_queue_2s, jitter_queue(2500), jitter_queue(3000)),
)


async def declare_topology(connection: RabbitBroker) -> None:
    dlx = await connection.declare_exchange(webhook_dlx)
    dlq = await connection.declare_queue(webhook_dlq)
    await dlq.bind(dlx, routing_key=webhook_dlq.routing_key)
    exchange = await connection.declare_exchange(payment_exchange)
    queue = await connection.declare_queue(payment_queue)
    await queue.bind(exchange, routing_key=payment_queue.routing_key)
    retry_exchange = await connection.declare_exchange(payment_retry_exchange)
    for group in RETRY_QUEUES:
        for definition in group:
            retry = await connection.declare_queue(definition)
            await retry.bind(retry_exchange, routing_key=definition.routing_key)
