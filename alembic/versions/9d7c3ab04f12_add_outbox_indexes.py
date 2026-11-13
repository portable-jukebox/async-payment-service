"""Partial indexes for outbox publication and retention."""

import sqlalchemy as sa

from alembic import op

revision = "9d7c3ab04f12"
down_revision = "6a912f20e4b8"
branch_labels = None
depends_on = None


def upgrade():
    op.create_index(
        "ix_outbox_pending",
        "outbox",
        ["event_type", "created_at", "id"],
        postgresql_where=sa.text("published_at IS NULL"),
    )
    op.create_index(
        "ix_outbox_published_cleanup",
        "outbox",
        ["published_at", "id"],
        postgresql_where=sa.text("published_at IS NOT NULL"),
    )


def downgrade():
    op.drop_index("ix_outbox_published_cleanup", table_name="outbox")
    op.drop_index("ix_outbox_pending", table_name="outbox")
