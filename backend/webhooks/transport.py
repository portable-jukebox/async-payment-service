"""Webhook egress: validate DNS at every TCP connect and pin the checked IP."""

import asyncio
import ipaddress
import socket
import ssl

import httpcore
import httpx


class UnsafeWebhookURL(ValueError):
    pass


def check_ip(value: str) -> None:
    address = ipaddress.ip_address(value)
    # RFC 6052's well-known NAT64 prefix can embed a non-public IPv4 address.
    if address in ipaddress.ip_network("64:ff9b::/96"):
        check_ip(str(ipaddress.IPv4Address(int(address) & 0xFFFFFFFF)))
    if (
        not address.is_global
        or address.is_multicast
        or getattr(address, "is_site_local", False)
        or getattr(address, "ipv4_mapped", None)
        or getattr(address, "sixtofour", None)
        or getattr(address, "teredo", None)
    ):
        raise UnsafeWebhookURL("Webhook destination must be a public unicast address")


def check_host(host: str) -> None:
    normalized = host.strip("[]").lower().rstrip(".")
    if (
        not normalized
        or "%" in normalized
        or normalized == "localhost"
        or normalized.endswith(".localhost")
    ):
        raise UnsafeWebhookURL("Forbidden webhook hostname")
    try:
        ipaddress.ip_address(normalized)
    except ValueError:
        return  # DNS is checked at the actual connection, never trusted here.
    check_ip(normalized)


def validate_webhook_url(value):
    url = httpx.URL(str(value))
    if url.scheme not in {"http", "https"} or url.userinfo:
        raise UnsafeWebhookURL("Webhook requires HTTP(S) without user information")
    check_host(url.host)
    return value


class PublicNetworkBackend(httpcore.AsyncNetworkBackend):
    def __init__(self):
        self.backend = httpcore.AnyIOBackend()

    async def connect_tcp(
        self, host, port, timeout=None, local_address=None, socket_options=None
    ):
        check_host(host)
        try:
            async with asyncio.timeout(
                min(timeout if timeout is not None else 5.0, 5.0)
            ):
                addresses = await asyncio.get_running_loop().getaddrinfo(
                    host, port, type=socket.SOCK_STREAM
                )
                if not addresses:
                    raise UnsafeWebhookURL("Webhook DNS returned no addresses")
                for _, _, _, _, address in addresses:
                    check_ip(address[0])
                # Never pass the hostname to the underlying dialer: no second DNS lookup.
                return await self.backend.connect_tcp(
                    addresses[0][4][0],
                    port,
                    timeout=timeout,
                    local_address=local_address,
                    socket_options=socket_options,
                )
        except TimeoutError as exc:
            raise httpcore.ConnectTimeout("Webhook connection timed out") from exc
        except OSError as exc:
            raise httpcore.ConnectError("Webhook DNS/connection failed") from exc

    async def connect_unix_socket(self, *args, **kwargs):
        raise UnsafeWebhookURL("Unix socket webhook connections are forbidden")

    async def sleep(self, seconds):
        await asyncio.sleep(seconds)


ERROR_MAP = {
    getattr(httpcore, name): getattr(httpx, name)
    for name in (
        "ConnectTimeout",
        "ReadTimeout",
        "WriteTimeout",
        "PoolTimeout",
        "ConnectError",
        "ReadError",
        "WriteError",
        "RemoteProtocolError",
        "LocalProtocolError",
        "UnsupportedProtocol",
    )
}


class WebhookTransport(httpx.AsyncBaseTransport):
    def __init__(self):
        self.pool = httpcore.AsyncConnectionPool(
            ssl_context=ssl.create_default_context(),
            network_backend=PublicNetworkBackend(),
            retries=0,
            max_connections=10,
        )

    async def handle_async_request(self, request):
        validate_webhook_url(request.url)
        # Do not accept caller-supplied TLS/SNI overrides or proxy routing.
        core_request = httpcore.Request(
            method=request.method,
            url=httpcore.URL(
                scheme=request.url.raw_scheme,
                host=request.url.raw_host,
                port=request.url.port,
                target=request.url.raw_path,
            ),
            headers=request.headers.raw,
            content=request.stream,
            extensions={"timeout": request.extensions.get("timeout", {})},
        )
        try:
            response = await self.pool.handle_async_request(core_request)
            try:
                # Only status/headers matter; do not buffer an untrusted response body.
                return httpx.Response(
                    response.status, headers=response.headers, content=b""
                )
            finally:
                await response.aclose()
        except tuple(ERROR_MAP) as exc:
            error_type = next(
                value for key, value in ERROR_MAP.items() if isinstance(exc, key)
            )
            raise error_type("Webhook transport failed", request=request) from exc

    async def aclose(self):
        await self.pool.aclose()


def webhook_client() -> httpx.AsyncClient:
    return httpx.AsyncClient(
        transport=WebhookTransport(),
        trust_env=False,
        follow_redirects=False,
        timeout=httpx.Timeout(10.0, connect=5.0),
    )
