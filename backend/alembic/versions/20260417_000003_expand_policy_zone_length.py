"""expand policy zone length

Revision ID: 20260417_000003
Revises: 20260417_000002
Create Date: 2026-04-17 00:00:03
"""
from __future__ import annotations

from alembic import op
import sqlalchemy as sa


revision = "20260417_000003"
down_revision = "20260417_000002"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.alter_column(
        "policies",
        "zone_sismique",
        existing_type=sa.String(length=4),
        type_=sa.String(length=8),
        existing_nullable=False,
    )


def downgrade() -> None:
    op.alter_column(
        "policies",
        "zone_sismique",
        existing_type=sa.String(length=8),
        type_=sa.String(length=4),
        existing_nullable=False,
    )
