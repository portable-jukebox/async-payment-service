import asyncio
from decimal import Decimal
from uuid import uuid4

import asyncpg
import pytest
from alembic.config import Config
from alembic.script import ScriptDirectory
from sqlalchemy import CheckConstraint
from sqlalchemy.exc import IntegrityError

from alembic import command
from backend.payments.models import Payment
from backend.tests.infrastructure import ROOT, validate_database_url

PREVIOUS_REVISION = "ff31925d1df3"
REVISION = "6a912f20e4b8"
VALUE_CONSTRAINTS = {"ck_payments_amount_positive", "ck_payments_currency"}
INSERT_PAYMENT = """
    INSERT INTO payments
        (id, amount, currency, status, description, webhook_url, metadata, idempotency_key)
    VALUES ($1, $2, $3, 'pending', 'Existing payment', 'https://example.com/hook',
            '{"order": 1}'::jsonb, $4)
"""


@pytest.fixture
def migration_config(test_database_url):
    validate_database_url(test_database_url)
    config = Config(str(ROOT / "alembic.ini"))
    config.attributes["database_url"] = test_database_url
    return config


def query(url, statement, *args):
    validate_database_url(url)

    async def execute():
        connection = await asyncpg.connect(
            url.replace("postgresql+asyncpg://", "postgresql://"), timeout=5
        )
        try:
            return [dict(row) for row in await connection.fetch(statement, *args)]
        finally:
            await connection.close()

    return asyncio.run(execute())


def constraint_names(url):
    return {
        row["conname"]
        for row in query(
            url,
            "SELECT conname FROM pg_constraint WHERE conrelid = 'payments'::regclass "
            "AND contype = 'c'",
        )
    }


def test_clean_migration_matches_models(migration_config, test_database_url):
    command.upgrade(migration_config, "head")
    command.check(migration_config)
    assert query(test_database_url, "SELECT version_num FROM alembic_version") == [
        {
            "version_num": ScriptDirectory.from_config(
                migration_config
            ).get_current_head()
        }
    ]
    model_checks = {
        check.name
        for check in Payment.__table__.constraints
        if isinstance(check, CheckConstraint)
    }
    assert VALUE_CONSTRAINTS <= model_checks
    assert constraint_names(test_database_url) == VALUE_CONSTRAINTS | {
        "ck_payments_status",
        "ck_payments_amount_finite",
    }


def test_upgrade_and_downgrade_preserve_existing_data(
    migration_config, test_database_url
):
    command.upgrade(migration_config, PREVIOUS_REVISION)
    for currency in ("RUB", "USD", "EUR"):
        query(test_database_url, INSERT_PAYMENT, uuid4(), 100, currency, currency)
    query(
        test_database_url,
        "INSERT INTO outbox (id, event_type, payload) VALUES ($1, 'payment.created', '{}')",
        uuid4(),
    )
    payments = query(test_database_url, "SELECT * FROM payments ORDER BY id")
    outbox = query(test_database_url, "SELECT * FROM outbox")

    for target in (REVISION, PREVIOUS_REVISION, REVISION):
        if target == PREVIOUS_REVISION:
            command.downgrade(migration_config, target)
        else:
            command.upgrade(migration_config, target)
        assert (
            query(test_database_url, "SELECT * FROM payments ORDER BY id") == payments
        )
        assert query(test_database_url, "SELECT * FROM outbox") == outbox
        expected = {"ck_payments_status"}
        if target == REVISION:
            expected |= VALUE_CONSTRAINTS
        assert constraint_names(test_database_url) == expected
        assert query(test_database_url, "SELECT version_num FROM alembic_version") == [
            {"version_num": target}
        ]


@pytest.mark.parametrize(
    ("amount", "currency", "constraint"),
    [
        (0, "RUB", "ck_payments_amount_positive"),
        (-1, "RUB", "ck_payments_amount_positive"),
        (100, "GBP", "ck_payments_currency"),
    ],
)
def test_invalid_existing_data_aborts_migration_without_changes(
    migration_config, test_database_url, amount, currency, constraint
):
    command.upgrade(migration_config, PREVIOUS_REVISION)
    query(
        test_database_url, INSERT_PAYMENT, uuid4(), amount, currency, "legacy-invalid"
    )
    before = query(test_database_url, "SELECT * FROM payments")
    with pytest.raises(IntegrityError, match=constraint):
        command.upgrade(migration_config, REVISION)
    assert query(test_database_url, "SELECT * FROM payments") == before
    assert constraint_names(test_database_url) == {"ck_payments_status"}
    assert query(test_database_url, "SELECT version_num FROM alembic_version") == [
        {"version_num": PREVIOUS_REVISION}
    ]


@pytest.mark.parametrize(
    ("amount", "currency", "constraint"),
    [
        (0, "RUB", "ck_payments_amount_positive"),
        (-1, "RUB", "ck_payments_amount_positive"),
        (100, "GBP", "ck_payments_currency"),
    ],
)
def test_constraints_enforced_for_direct_sql(
    migration_config, test_database_url, amount, currency, constraint
):
    command.upgrade(migration_config, "head")
    with pytest.raises(asyncpg.CheckViolationError) as caught:
        query(test_database_url, INSERT_PAYMENT, uuid4(), amount, currency, "invalid")
    assert caught.value.constraint_name == constraint
    assert query(test_database_url, "SELECT count(*) AS n FROM payments") == [{"n": 0}]


def test_nan_is_rejected_on_clean_database(migration_config, test_database_url):
    command.upgrade(migration_config, "head")
    with pytest.raises(asyncpg.CheckViolationError) as caught:
        query(test_database_url, INSERT_PAYMENT, uuid4(), Decimal("NaN"), "USD", "nan")
    assert caught.value.constraint_name == "ck_payments_amount_finite"


def test_nan_existing_data_aborts_new_migration(migration_config, test_database_url):
    command.upgrade(migration_config, "9d7c3ab04f12")
    query(test_database_url, INSERT_PAYMENT, uuid4(), Decimal("NaN"), "USD", "nan")
    with pytest.raises(IntegrityError, match="ck_payments_amount_finite"):
        command.upgrade(migration_config, "head")
    assert query(test_database_url, "SELECT amount::text FROM payments") == [
        {"amount": "NaN"}
    ]
    assert query(test_database_url, "SELECT version_num FROM alembic_version") == [
        {"version_num": "9d7c3ab04f12"}
    ]
