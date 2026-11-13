import asyncio
import hashlib
import hmac
import json
import time

import httpx

from backend.core.config import Settings
from backend.core.metrics import webhooks
from backend.webhooks.transport import UnsafeWebhookURL

settings = Settings()


def signed_payload(payload: dict) -> tuple[bytes, dict[str, str]]:
    body = json.dumps(payload, separators=(",", ":"), ensure_ascii=False).encode()
    timestamp = str(int(time.time()))
    signature = hmac.new(
        settings.signing_key(), timestamp.encode() + b"." + body, hashlib.sha256
    ).hexdigest()
    return body, {
        "Content-Type": "application/json",
        "Idempotency-Key": payload["payment_id"],
        "X-Webhook-Timestamp": timestamp,
        "X-Webhook-Signature": f"v1={signature}",
    }


async def deliver_webhook(
    http_client: httpx.AsyncClient,
    webhook_url: str,
    payload: dict,
) -> None:
    body, headers = signed_payload(payload)
    try:
        async with asyncio.timeout(15):
            response = await http_client.post(
                url=webhook_url, content=body, headers=headers, follow_redirects=False
            )
    except TimeoutError as exc:
        webhooks.labels("network_error").inc()
        raise httpx.ReadTimeout("Webhook total deadline exceeded") from exc
    except httpx.TransportError:
        webhooks.labels("network_error").inc()
        raise
    except UnsafeWebhookURL:
        webhooks.labels("blocked").inc()
        raise
    try:
        response.raise_for_status()
    except httpx.HTTPStatusError:
        webhooks.labels("http_error").inc()
        raise
    webhooks.labels("success").inc()
