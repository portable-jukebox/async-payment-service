import asyncio
import subprocess
from uuid import uuid4

import asyncpg
import pytest
import pytest_asyncio
from alembic.config import Config
from sqlalchemy.engine import make_url
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

from alembic import command
from backend.tests.infrastructure import (
    COMPOSE,
    DATABASE_URL,
    RABBITMQ_URL,
    ROOT,
    configure_test_environment,
    require_test_container,
    validate_database_url,
)


def pytest_configure(config):
    configure_test_environment()


def pytest_collection_modifyitems(items):
    for item in items:
        if {"test_database_url", "isolated_rabbitmq_url"}.intersection(
            item.fixturenames
        ):
            item.add_marker(pytest.mark.integration)
        if "isolated_rabbitmq_url" in item.fixturenames:
            item.add_marker(pytest.mark.rabbitmq)


@pytest.fixture(scope="session")
def isolated_postgres():
    require_test_container("db")


@pytest.fixture(scope="session")
def isolated_rabbitmq_url():
    require_test_container("rabbitmq")
    return RABBITMQ_URL


@pytest.fixture
def worker_rabbitmq_url(isolated_rabbitmq_url):
    name = f"pytest_{uuid4().hex}_test"

    def control(*args):
        subprocess.run(
            [*COMPOSE, "exec", "-T", "rabbitmq", "rabbitmqctl", *args],
            check=True,
            capture_output=True,
            timeout=20,
        )

    control("add_vhost", name)
    try:
        control("set_permissions", "-p", name, "test_runner", ".*", ".*", ".*")
        yield isolated_rabbitmq_url.rsplit("/", 1)[0] + "/" + name
    finally:
        control("delete_vhost", name)


@pytest.fixture
def test_database_url(isolated_postgres):
    name = f"pytest_{uuid4().hex}_test"
    url = (
        make_url(DATABASE_URL).set(database=name).render_as_string(hide_password=False)
    )
    validate_database_url(url)

    async def database_command(command):
        connection = await asyncpg.connect(
            DATABASE_URL.replace("postgresql+asyncpg://", "postgresql://"), timeout=5
        )
        try:
            await connection.execute(command)
        finally:
            await connection.close()

    asyncio.run(database_command(f'CREATE DATABASE "{name}"'))
    try:
        yield url
    finally:
        # Only the exact UUID database created by this fixture can be removed.
        asyncio.run(database_command(f'DROP DATABASE "{name}"'))


@pytest_asyncio.fixture
async def test_engine(test_database_url, migrated_database):
    test_engine = create_async_engine(test_database_url)
    try:
        yield test_engine
    finally:
        await test_engine.dispose()


@pytest.fixture
def migrated_database(test_database_url):
    validate_database_url(test_database_url)
    config = Config(str(ROOT / "alembic.ini"))
    config.attributes["database_url"] = test_database_url
    command.upgrade(config, "head")


@pytest_asyncio.fixture
async def test_session(test_engine):
    async_factory = async_sessionmaker(test_engine, expire_on_commit=False)
    async with async_factory() as session:
        yield session
