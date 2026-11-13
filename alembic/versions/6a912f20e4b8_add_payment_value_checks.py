"""Add currency and positive amount checks without rewriting existing data.

Revision ID: 6a912f20e4b8
Revises: ff31925d1df3
"""

from alembic import op

revision = "6a912f20e4b8"
down_revision = "ff31925d1df3"
branch_labels = None
depends_on = None


def upgrade() -> None:
    # PostgreSQL validates existing rows; a violation aborts this migration
    # transaction with the offending constraint's name. No data is modified.
    op.create_check_constraint("ck_payments_amount_positive", "payments", "amount > 0")
    op.create_check_constraint(
        "ck_payments_currency", "payments", "currency IN ('RUB', 'USD', 'EUR')"
    )


def downgrade() -> None:
    op.drop_constraint("ck_payments_currency", "payments", type_="check")
    op.drop_constraint("ck_payments_amount_positive", "payments", type_="check")
