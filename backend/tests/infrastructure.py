"""Fail-closed infrastructure guards; never load connection settings from .env."""

import json
import os
import re
import subprocess
from pathlib import Path

from sqlalchemy.engine import make_url

ROOT = Path(__file__).resolve().parents[2]
PROJECT = "payment-service-async-tests"
DATABASE_URL = (
    "postgresql+asyncpg://test_runner:isolated-test-only@127.0.0.1:55433/payments_test"
)
RABBITMQ_URL = "amqp://test_runner:isolated-test-only@127.0.0.1:55673/payments_test"
COMPOSE = ["docker", "compose", "-f", str(ROOT / "compose.test.yaml"), "-p", PROJECT]


def configure_test_environment() -> None:
    # App imports must not inherit production credentials or connect by accident.
    os.environ.update(
        DATABASE_URL="postgresql+asyncpg://disabled:disabled@127.0.0.1:1/disabled",
        TEST_DATABASE_URL=DATABASE_URL,
        RABBITMQ_URL="amqp://disabled:disabled@127.0.0.1:1/disabled",
        API_KEY="isolated-test-api-key",
        WEBHOOK_SIGNING_SECRET="isolated-test-signing-key-32-bytes-minimum",
        METRICS_ENABLED="false",
    )


def validate_database_url(value: str) -> str:
    url = make_url(value)
    expected = make_url(DATABASE_URL)
    if url.set(database=expected.database) != expected:
        raise ValueError("Only the isolated test PostgreSQL endpoint is allowed")
    name = url.database or ""
    if name != "payments_test" and not re.fullmatch(r"pytest_[0-9a-f]{32}_test", name):
        raise ValueError("Not an owned test database name")
    return name


def validate_container(info: dict, service: str) -> None:
    port, target, destination = {
        "db": ("55433", "5432/tcp", "/var/lib/postgresql"),
        "rabbitmq": ("55673", "5672/tcp", "/var/lib/rabbitmq"),
    }[service]
    labels = info.get("Config", {}).get("Labels", {})
    if (
        labels.get("com.docker.compose.project") != PROJECT
        or labels.get("com.docker.compose.service") != service
    ):
        raise RuntimeError("Container does not belong to the isolated test project")
    mounts = info.get("Mounts", [])
    if any(mount.get("Type") != "tmpfs" for mount in mounts):
        raise RuntimeError(
            "Test containers must use tmpfs only, never persistent mounts"
        )
    tmpfs = info.get("HostConfig", {}).get("Tmpfs") or {}
    if destination not in tmpfs and not any(
        mount.get("Destination") == destination for mount in mounts
    ):
        raise RuntimeError("Test data directory is not backed by tmpfs")
    bindings = info.get("NetworkSettings", {}).get("Ports", {}).get(target)
    if bindings != [{"HostIp": "127.0.0.1", "HostPort": port}]:
        raise RuntimeError("Unexpected test container port mapping")
    if info.get("State", {}).get("Health", {}).get("Status") != "healthy":
        raise RuntimeError("Isolated test container is not healthy")


def require_test_container(service: str) -> None:
    try:
        result = subprocess.run(
            [*COMPOSE, "ps", "-q", service],
            check=True,
            capture_output=True,
            text=True,
            timeout=15,
        )
        identifiers = result.stdout.split()
        if len(identifiers) != 1:
            raise RuntimeError("Expected exactly one isolated test container")
        result = subprocess.run(
            ["docker", "inspect", identifiers[0]],
            check=True,
            capture_output=True,
            text=True,
            timeout=15,
        )
        validate_container(json.loads(result.stdout)[0], service)
    except (OSError, subprocess.SubprocessError) as exc:
        raise RuntimeError(
            "Isolated Docker infrastructure unavailable. Run: docker compose "
            "-f compose.test.yaml -p payment-service-async-tests up -d --wait"
        ) from exc
