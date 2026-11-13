import asyncio
import hashlib
import hmac
import json
import socket
import ssl
import subprocess
from unittest.mock import AsyncMock

import httpcore
import httpx
import pytest
from pydantic import ValidationError

from backend.core.config import Settings
from backend.payments.schemas import PaymentCreate
from backend.tests.infrastructure import COMPOSE
from backend.webhooks.service import deliver_webhook
from backend.webhooks.transport import (
    PublicNetworkBackend,
    UnsafeWebhookURL,
    webhook_client,
)


def dns(*addresses):
    return [
        (
            socket.AF_INET6 if ":" in ip else socket.AF_INET,
            socket.SOCK_STREAM,
            6,
            "",
            (ip, 443),
        )
        for ip in addresses
    ]


@pytest.mark.parametrize(
    "host",
    [
        "127.0.0.1",
        "10.0.0.1",
        "172.16.0.1",
        "192.168.1.1",
        "100.64.0.1",
        "169.254.169.254",
        "0.0.0.0",
        "224.0.0.1",
        "[::1]",
        "[fe80::1]",
        "[fc00::1]",
        "[64:ff9b::127.0.0.1]",
        "[64:ff9b::10.0.0.1]",
        "[::ffff:127.0.0.1]",
        "localhost",
        "LOCALHOST.",
        "sub.localhost",
        "user:password@example.com",
    ],
)
def test_api_rejects_unsafe_webhook(host):
    with pytest.raises(ValidationError):
        PaymentCreate(
            amount="1",
            currency="RUB",
            description="test",
            metadata={},
            webhook_url=f"http://{host}/",
        )


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "addresses",
    [("127.0.0.1",), ("93.184.216.34", "10.0.0.1"), ("64:ff9b::7f00:1",), ()],
)
async def test_unsafe_dns_never_connects(monkeypatch, addresses):
    resolver = AsyncMock(return_value=dns(*addresses))
    monkeypatch.setattr(asyncio.get_running_loop(), "getaddrinfo", resolver)
    backend = PublicNetworkBackend()
    connect = AsyncMock()
    monkeypatch.setattr(backend.backend, "connect_tcp", connect)
    with pytest.raises(UnsafeWebhookURL):
        await backend.connect_tcp("example.com", 443)
    connect.assert_not_awaited()


@pytest.mark.asyncio
async def test_rebinding_is_checked_at_each_connection(monkeypatch):
    resolver = AsyncMock(side_effect=[dns("93.184.216.34"), dns("127.0.0.1")])
    monkeypatch.setattr(asyncio.get_running_loop(), "getaddrinfo", resolver)
    backend = PublicNetworkBackend()
    connect = AsyncMock()
    monkeypatch.setattr(backend.backend, "connect_tcp", connect)
    await backend.connect_tcp("example.com", 443)
    assert connect.await_args.args == ("93.184.216.34", 443)
    with pytest.raises(UnsafeWebhookURL):
        await backend.connect_tcp("example.com", 443)
    assert connect.await_count == 1
    assert resolver.await_count == 2


@pytest.mark.asyncio
@pytest.mark.parametrize("error", [socket.gaierror("unavailable"), TimeoutError()])
async def test_dns_failure_has_no_fallback(monkeypatch, error):
    monkeypatch.setattr(
        asyncio.get_running_loop(), "getaddrinfo", AsyncMock(side_effect=error)
    )
    backend = PublicNetworkBackend()
    connect = AsyncMock()
    monkeypatch.setattr(backend.backend, "connect_tcp", connect)
    with pytest.raises((httpcore.ConnectError, httpcore.ConnectTimeout)):
        await backend.connect_tcp("example.com", 443)
    connect.assert_not_awaited()


@pytest.mark.asyncio
async def test_production_transport_preserves_tls_and_host_and_ignores_proxies(
    monkeypatch,
):
    monkeypatch.setenv("HTTPS_PROXY", "http://127.0.0.1:9")
    monkeypatch.setenv("ALL_PROXY", "http://127.0.0.1:9")
    monkeypatch.setattr(
        asyncio.get_running_loop(),
        "getaddrinfo",
        AsyncMock(return_value=dns("93.184.216.34")),
    )
    stream = httpcore.AsyncMockStream(
        [b"HTTP/1.1 200 OK\r\nContent-Length: 0\r\nConnection: close\r\n\r\n"]
    )
    start_tls = AsyncMock(return_value=stream)
    write = AsyncMock()
    monkeypatch.setattr(stream, "start_tls", start_tls)
    monkeypatch.setattr(stream, "write", write)
    connect = AsyncMock(return_value=stream)
    monkeypatch.setattr(httpcore.AnyIOBackend, "connect_tcp", connect)
    async with webhook_client() as client:
        response = await client.get(
            "https://example.com/test", extensions={"sni_hostname": "evil.example"}
        )
    assert response.status_code == 200
    assert connect.await_args.args == ("93.184.216.34", 443)
    assert start_tls.await_args.kwargs["server_hostname"] == "example.com"
    context = start_tls.await_args.kwargs["ssl_context"]
    assert context.check_hostname and context.verify_mode == ssl.CERT_REQUIRED
    assert b"Host: example.com" in b"".join(
        call.args[0] for call in write.await_args_list
    )


@pytest.mark.asyncio
async def test_redirect_not_followed_and_signature_matches_exact_body(monkeypatch):
    monkeypatch.setattr("backend.webhooks.service.time.time", lambda: 1700000000)
    requests = []

    def receive(request):
        requests.append(request)
        return httpx.Response(302, headers={"Location": "http://127.0.0.1/secret"})

    payload = {"payment_id": "abc", "status": "succeeded", "note": "тест"}
    async with httpx.AsyncClient(
        transport=httpx.MockTransport(receive), follow_redirects=True
    ) as client:
        with pytest.raises(httpx.HTTPStatusError):
            await deliver_webhook(client, "https://example.com/webhook", payload)
    assert len(requests) == 1
    request = requests[0]
    assert json.loads(request.content) == payload
    expected = hmac.new(
        Settings().signing_key(), b"1700000000." + request.content, hashlib.sha256
    ).hexdigest()
    assert request.headers["X-Webhook-Signature"] == f"v1={expected}"
    assert request.headers["X-Webhook-Timestamp"] == "1700000000"
    assert request.headers["Idempotency-Key"] == "abc"
    for changed in (
        b"1700000001." + request.content,
        b"1700000000." + request.content + b" ",
    ):
        assert (
            hmac.new(Settings().signing_key(), changed, hashlib.sha256).hexdigest()
            != expected
        )


@pytest.mark.parametrize("secret", [None, "", "short"])
def test_signing_secret_required(secret):
    with pytest.raises(ValueError, match="WEBHOOK_SIGNING_SECRET"):
        Settings(webhook_signing_secret=secret).signing_key()


@pytest.fixture
def tls_files(isolated_postgres, tmp_path):
    # Ephemeral test-only key/certificate, generated inside the isolated container.
    result = subprocess.run(
        [
            *COMPOSE,
            "exec",
            "-T",
            "db",
            "openssl",
            "req",
            "-x509",
            "-newkey",
            "rsa:2048",
            "-nodes",
            "-keyout",
            "/dev/stdout",
            "-out",
            "/dev/stdout",
            "-days",
            "1",
            "-subj",
            "/CN=example.com",
            "-addext",
            "subjectAltName=DNS:example.com",
        ],
        capture_output=True,
        check=True,
        timeout=20,
    )
    key, cert = tmp_path / "key.pem", tmp_path / "cert.pem"
    contents = result.stdout
    start = contents.index(b"-----BEGIN CERTIFICATE-----")
    key.write_bytes(contents[:start])
    cert.write_bytes(contents[start:])
    return key, cert


@pytest.mark.integration
@pytest.mark.asyncio
@pytest.mark.parametrize("case", ["trusted", "untrusted", "wrong_hostname"])
async def test_real_tls_handshake_verifies_certificate_and_hostname(
    tls_files, monkeypatch, case
):
    key, cert = tls_files
    server_context = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
    server_context.load_cert_chain(cert, key)
    names = []
    server_context.set_servername_callback(
        lambda socket, name, context: names.append(name)
    )

    async def respond(reader, writer):
        try:
            await reader.readuntil(b"\r\n\r\n")
            writer.write(
                b"HTTP/1.1 200 OK\r\nContent-Length: 0\r\nConnection: close\r\n\r\n"
            )
            await writer.drain()
        finally:
            writer.close()
            await writer.wait_closed()

    server = await asyncio.start_server(respond, "127.0.0.1", 0, ssl=server_context)
    port = server.sockets[0].getsockname()[1]
    original_connect = httpcore.AnyIOBackend.connect_tcp
    original_resolve = asyncio.get_running_loop().getaddrinfo

    async def resolve(host, requested_port, **kwargs):
        if host in {"example.com", "wrong.example"}:
            return dns("93.184.216.34")
        return await original_resolve(host, requested_port, **kwargs)

    async def local_wire(self, host, requested_port, **kwargs):
        # Only the physical test wire is redirected; production DNS/IP policy stays active.
        assert host == "93.184.216.34"
        return await original_connect(self, "127.0.0.1", port, **kwargs)

    monkeypatch.setattr(asyncio.get_running_loop(), "getaddrinfo", resolve)
    monkeypatch.setattr(httpcore.AnyIOBackend, "connect_tcp", local_wire)
    if case != "untrusted":
        original_context = ssl.create_default_context

        def trust_test_ca():
            context = original_context()
            context.load_verify_locations(cafile=cert)
            return context

        monkeypatch.setattr(
            "backend.webhooks.transport.ssl.create_default_context", trust_test_ca
        )
    hostname = "wrong.example" if case == "wrong_hostname" else "example.com"
    async with server, webhook_client() as client:
        if case == "trusted":
            assert (await client.get(f"https://{hostname}/")).status_code == 200
        else:
            with pytest.raises(httpx.ConnectError):
                await client.get(f"https://{hostname}/")
    assert names == [hostname]
