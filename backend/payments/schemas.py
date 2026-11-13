from datetime import datetime
from decimal import Decimal
from typing import Annotated, Literal
from uuid import UUID

from pydantic import AfterValidator, BaseModel, Field, HttpUrl, JsonValue

from backend.webhooks.transport import validate_webhook_url


class PaymentCreate(BaseModel):
    amount: Decimal = Field(gt=0, max_digits=18, decimal_places=2)
    currency: Literal["RUB", "USD", "EUR"]
    description: str = Field(max_length=128)
    metadata: dict[str, JsonValue]
    webhook_url: Annotated[HttpUrl, AfterValidator(validate_webhook_url)]


class PaymentAccepted(BaseModel):
    payment_id: UUID
    status: Literal["pending", "succeeded", "failed"]
    created_at: datetime


class PaymentRead(BaseModel):
    payment_id: UUID
    amount: Decimal
    currency: Literal["RUB", "USD", "EUR"]
    status: Literal["pending", "succeeded", "failed"]
    description: str
    metadata: dict[str, JsonValue]
    webhook_url: str
    idempotency_key: str
    created_at: datetime
    processed_at: datetime | None
