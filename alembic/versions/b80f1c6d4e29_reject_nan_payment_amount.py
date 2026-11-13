"""Reject numeric NaN, which PostgreSQL considers greater than zero."""

from alembic import op

revision = "b80f1c6d4e29"
down_revision = "9d7c3ab04f12"
branch_labels = None
depends_on = None


def upgrade():
    op.create_check_constraint(
        "ck_payments_amount_finite", "payments", "amount <> 'NaN'::numeric"
    )


def downgrade():
    op.drop_constraint("ck_payments_amount_finite", "payments", type_="check")
