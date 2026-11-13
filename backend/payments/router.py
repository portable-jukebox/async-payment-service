from typing import Annotated
from uuid import UUID

from fastapi import APIRouter, Depends, Header, HTTPException, status
from pydantic import AfterValidator
from sqlalchemy.ext.asyncio import AsyncSession

from backend.db.session import get_async_session
from backend.payments.schemas import PaymentAccepted, PaymentCreate, PaymentRead
from backend.payments.service import (
    IdempotencyConflictError,
    create_payment,
    get_payment,
)

router = APIRouter(prefix="/payments", tags=["payments"])


def validate_idempotency_key(value: str) -> str:
    if not value.strip():
        raise ValueError("Idempotency-Key must contain a non-whitespace character")
    return value


@router.post(
    "",
    response_model=PaymentAccepted,
    status_code=status.HTTP_202_ACCEPTED,
)
async def create_payment_endpoint(
    data: PaymentCreate,
    idempotency_key: Annotated[
        str,
        Header(min_length=1, max_length=255),
        AfterValidator(validate_idempotency_key),
    ],
    session: Annotated[AsyncSession, Depends(get_async_session)],
):
    try:
        payment = await create_payment(session, data, idempotency_key)

    except IdempotencyConflictError:
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail="Idempotency key conflict",
        )

    return PaymentAccepted(
        payment_id=payment.id,
        status=payment.status,
        created_at=payment.created_at,
    )


@router.get(
    "/{payment_id}",
    response_model=PaymentRead,
)
async def get_payment_endpoint(
    payment_id: UUID, session: Annotated[AsyncSession, Depends(get_async_session)]
):
    payment = await get_payment(session, payment_id)

    if payment is None:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND, detail="Payment not found"
        )

    return PaymentRead(
        payment_id=payment.id,
        amount=payment.amount,
        currency=payment.currency,
        status=payment.status,
        description=payment.description,
        webhook_url=payment.webhook_url,
        metadata=payment._metadata,
        idempotency_key=payment.idempotency_key,
        created_at=payment.created_at,
        processed_at=payment.processed_at,
    )
