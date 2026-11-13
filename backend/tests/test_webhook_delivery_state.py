from datetime import UTC, datetime
from uuid import uuid4

import pytest

from backend.payments.models import Payment
from backend.payments.service import (
    complete_payment,
    get_payment_processing_state,
    mark_webhook_delivered,
)


@pytest.mark.asyncio
async def test_webhook_delivery_state(test_session):
    payment = Payment(
        amount="100.00",
        currency="RUB",
        status="succeeded",
        description="Webhook delivery test",
        _metadata={},
        webhook_url="https://example.com/webhook",
        idempotency_key=str(uuid4()),
        processed_at=datetime.now(UTC),
    )
    test_session.add(payment)
    await test_session.commit()

    state = await get_payment_processing_state(test_session, payment.id)
    assert state == (False, False)

    await mark_webhook_delivered(test_session, payment.id)

    first_delivery_time = payment.webhook_delivered_at
    assert first_delivery_time is not None

    state = await get_payment_processing_state(test_session, payment.id)
    assert state == (False, True)

    result = await complete_payment(test_session, payment.id)
    assert result is None

    await mark_webhook_delivered(test_session, payment.id)

    assert payment.webhook_delivered_at == first_delivery_time
