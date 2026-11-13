from copy import deepcopy
from unittest.mock import Mock

import pytest
from sqlalchemy.engine import make_url

from backend.core.config import Settings
from backend.tests import conftest
from backend.tests.infrastructure import (
    DATABASE_URL,
    PROJECT,
    configure_test_environment,
    validate_container,
    validate_database_url,
)


@pytest.mark.parametrize(
    "changes",
    [
        {"host": "db"},
        {"host": "localhost"},
        {"port": 5433},
        {"username": "admin"},
        {"password": "another-password"},
        {"database": "payments"},
        {"database": "another_test"},
        {"query": {"host": "db"}},
        {"drivername": "postgresql"},
    ],
)
def test_rejects_nonisolated_database_urls(changes):
    url = make_url(DATABASE_URL).set(**changes).render_as_string(hide_password=False)
    with pytest.raises(ValueError):
        validate_database_url(url)


def test_accepts_only_base_or_owned_database():
    assert validate_database_url(DATABASE_URL) == "payments_test"
    name = f"pytest_{'a' * 32}_test"
    url = (
        make_url(DATABASE_URL).set(database=name).render_as_string(hide_password=False)
    )
    assert validate_database_url(url) == name


def test_migration_refuses_unsafe_url_before_alembic(monkeypatch):
    upgrade = Mock()
    monkeypatch.setattr(conftest.command, "upgrade", upgrade)
    with pytest.raises(ValueError):
        conftest.migrated_database.__wrapped__(
            "postgresql+asyncpg://admin:example@127.0.0.1:5433/payments"
        )
    upgrade.assert_not_called()


def test_app_imports_cannot_inherit_working_connections(monkeypatch):
    for key in ("DATABASE_URL", "TEST_DATABASE_URL", "RABBITMQ_URL", "API_KEY"):
        monkeypatch.setenv(key, "must-not-be-used")
    configure_test_environment()
    settings = Settings()
    assert settings.test_database_url == DATABASE_URL
    assert make_url(settings.database_url).port == 1
    assert make_url(settings.rabbitmq_url).port == 1
    assert settings.api_key == "isolated-test-api-key"


def container_info(service):
    port, internal_port, destination = {
        "db": ("55433", "5432/tcp", "/var/lib/postgresql"),
        "rabbitmq": ("55673", "5672/tcp", "/var/lib/rabbitmq"),
    }[service]
    return {
        "Config": {
            "Labels": {
                "com.docker.compose.project": PROJECT,
                "com.docker.compose.service": service,
            }
        },
        "Mounts": [],
        "HostConfig": {"Tmpfs": {destination: ""}},
        "NetworkSettings": {
            "Ports": {internal_port: [{"HostIp": "127.0.0.1", "HostPort": port}]}
        },
        "State": {"Health": {"Status": "healthy"}},
    }


@pytest.mark.parametrize("service", ["db", "rabbitmq"])
@pytest.mark.parametrize("mount_format", ["host_config", "mounts"])
def test_accepts_isolated_tmpfs_container(service, mount_format):
    info = container_info(service)
    if mount_format == "mounts":
        destination = next(iter(info["HostConfig"].pop("Tmpfs")))
        info["Mounts"] = [{"Type": "tmpfs", "Destination": destination}]
    validate_container(info, service)


@pytest.mark.parametrize(
    "fault",
    ["project", "service", "volume", "bind", "missing_mount", "port", "unhealthy"],
)
def test_rejects_unsafe_or_unready_container(fault):
    info = deepcopy(container_info("db"))
    if fault in {"project", "service"}:
        info["Config"]["Labels"][f"com.docker.compose.{fault}"] = "working"
    elif fault in {"volume", "bind"}:
        info["Mounts"] = [{"Type": fault, "Destination": "/persistent-data"}]
    elif fault == "missing_mount":
        info["HostConfig"]["Tmpfs"] = {}
    elif fault == "port":
        info["NetworkSettings"]["Ports"]["5432/tcp"][0]["HostPort"] = "5433"
    else:
        info["State"]["Health"]["Status"] = "unhealthy"
    with pytest.raises(RuntimeError):
        validate_container(info, "db")
