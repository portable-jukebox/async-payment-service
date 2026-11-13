from uuid import UUID

from sqlalchemy import select
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.ext.asyncio import AsyncSession

from backend.payments.models import Payment


class PaymentRepository:
    def __init__(self, session: AsyncSession):
        self.session = session

    async def get_by_id(self, payment_id: UUID) -> Payment | None:
        stmt = select(Payment).where(Payment.id == payment_id)
        result = await self.session.execute(stmt)
        return result.scalar_one_or_none()

    async def get_by_idempotency_key(
        self,
        idempotency_key: str,
    ) -> Payment | None:
        stmt = select(Payment).where(Payment.idempotency_key == idempotency_key)
        result = await self.session.execute(stmt)
        return result.scalar_one_or_none()

    async def add_if_absent(self, payment: Payment) -> Payment | None:
        stmt = (
            insert(Payment)
            .values(
                amount=payment.amount,
                currency=payment.currency,
                description=payment.description,
                _metadata=payment._metadata,
                webhook_url=payment.webhook_url,
                idempotency_key=payment.idempotency_key,
            )
            .on_conflict_do_nothing(index_elements=[Payment.idempotency_key])
            .returning(Payment)
        )
        result = await self.session.execute(stmt)
        return result.scalar_one_or_none()

    async def get_by_id_for_update(
        self,
        payment_id: UUID,
    ) -> Payment | None:
        stmt = select(Payment).where(Payment.id == payment_id).with_for_update()

        result = await self.session.execute(stmt)
        return result.scalar_one_or_none()
