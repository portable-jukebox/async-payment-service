import random
from datetime import UTC, datetime
from uuid import UUID

from sqlalchemy.ext.asyncio import AsyncSession

from backend.core.metrics import completed
from backend.outbox.repository import OutboxRepository
from backend.payments.models import Payment
from backend.payments.repository import PaymentRepository
from backend.payments.schemas import PaymentCreate


class IdempotencyConflictError(Exception):
    pass


def payment_matches_data(
    payment: Payment,
    data: PaymentCreate,
) -> bool:
    return (
        payment.amount == data.amount
        and payment.currency == data.currency
        and payment.description == data.description
        and payment._metadata == data.metadata
        and payment.webhook_url == str(data.webhook_url)
    )


async def create_payment(
    session: AsyncSession,
    data: PaymentCreate,
    idempotency_key: str,
) -> Payment:
    payment_repo = PaymentRepository(session)
    outbox_repo = OutboxRepository(session)

    async with session.begin():
        payment = await payment_repo.add_if_absent(
            Payment(
                amount=data.amount,
                currency=data.currency,
                description=data.description,
                _metadata=data.metadata,
                webhook_url=str(data.webhook_url),
                idempotency_key=idempotency_key,
            )
        )
        if payment is not None:
            outbox_repo.add(
                event_type="payment.created",
                payload={"payment_id": str(payment.id)},
            )

        else:
            # A separate statement sees the concurrent winner after INSERT waits
            # for its commit under PostgreSQL's default READ COMMITTED isolation.
            payment = await payment_repo.get_by_idempotency_key(idempotency_key)
            if payment is None:
                raise RuntimeError("Conflicting payment disappeared")
            if not payment_matches_data(payment, data):
                raise IdempotencyConflictError()

    return payment


async def get_payment(
    session: AsyncSession,
    payment_id: UUID,
) -> Payment | None:
    return await PaymentRepository(session).get_by_id(payment_id)


async def complete_payment(
    session: AsyncSession,
    payment_id: UUID,
) -> tuple[str, dict] | None:
    repository = PaymentRepository(session)
    transitioned = False

    async with session.begin():
        payment = await repository.get_by_id_for_update(payment_id)

        if payment is None:
            raise ValueError(f"Payment {payment_id} not found")

        if payment.webhook_delivered_at is not None:
            return None

        if payment.status == "pending":
            transitioned = True
            payment.status = "succeeded" if random.random() < 0.9 else "failed"
            payment.processed_at = datetime.now(UTC)

        webhook_url = payment.webhook_url
        payload = {
            "payment_id": str(payment.id),
            "status": payment.status,
        }

    if transitioned:
        completed.labels(payload["status"]).inc()
    return webhook_url, payload


async def get_payment_processing_state(
    session: AsyncSession,
    payment_id: UUID,
) -> tuple[bool, bool]:
    repository = PaymentRepository(session)

    async with session.begin():
        payment = await repository.get_by_id(payment_id)

        if payment is None:
            raise ValueError(f"Payment {payment_id} not found")

        return (
            payment.status == "pending",
            payment.webhook_delivered_at is not None,
        )


async def mark_webhook_delivered(
    session: AsyncSession,
    payment_id: UUID,
) -> None:
    repository = PaymentRepository(session)

    async with session.begin():
        payment = await repository.get_by_id_for_update(payment_id)

        if payment is None:
            raise ValueError(f"Payment {payment_id} not found")

        if payment.status == "pending":
            raise ValueError("Cannot deliver webhook for pending payment")

        if payment.webhook_delivered_at is None:
            payment.webhook_delivered_at = datetime.now(UTC)
