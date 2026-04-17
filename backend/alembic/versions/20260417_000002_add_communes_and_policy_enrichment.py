"""add communes and policy enrichment

Revision ID: 20260417_000002
Revises: 20260417_000001
Create Date: 2026-04-17 00:00:02
"""
from __future__ import annotations

from alembic import op
import sqlalchemy as sa


revision = "20260417_000002"
down_revision = "20260417_000001"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "communes",
        sa.Column("id", sa.Integer(), autoincrement=True, nullable=False),
        sa.Column("wilaya_code", sa.String(length=2), nullable=False),
        sa.Column("wilaya_name", sa.String(length=120), nullable=False),
        sa.Column("code_commune", sa.String(length=20), nullable=True),
        sa.Column("commune_name", sa.String(length=120), nullable=False),
        sa.Column("zone_sismique", sa.String(length=8), nullable=False),
        sa.Column("zone_num", sa.Integer(), nullable=True),
        sa.Column("zone_source", sa.String(length=50), nullable=True),
        sa.Column("lat", sa.Numeric(precision=10, scale=7), nullable=True),
        sa.Column("lon", sa.Numeric(precision=10, scale=7), nullable=True),
        sa.Column("coordinate_source", sa.String(length=80), nullable=True),
        sa.Column("has_coordinates", sa.Boolean(), server_default=sa.text("false"), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), server_default=sa.text("now()"), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), server_default=sa.text("now()"), nullable=False),
        sa.PrimaryKeyConstraint("id"),
    )
    op.create_index(op.f("ix_communes_code_commune"), "communes", ["code_commune"], unique=False)
    op.create_index(op.f("ix_communes_commune_name"), "communes", ["commune_name"], unique=False)
    op.create_index(op.f("ix_communes_wilaya_code"), "communes", ["wilaya_code"], unique=False)
    op.create_index(op.f("ix_communes_wilaya_name"), "communes", ["wilaya_name"], unique=False)
    op.create_index(op.f("ix_communes_zone_sismique"), "communes", ["zone_sismique"], unique=False)

    op.add_column("policies", sa.Column("prime_rate", sa.Numeric(precision=12, scale=6), nullable=True))
    op.add_column("policies", sa.Column("lat", sa.Numeric(precision=10, scale=7), nullable=True))
    op.add_column("policies", sa.Column("lon", sa.Numeric(precision=10, scale=7), nullable=True))
    op.add_column("policies", sa.Column("zone_source", sa.String(length=50), nullable=True))
    op.add_column("policies", sa.Column("coordinate_source", sa.String(length=80), nullable=True))
    op.add_column("policies", sa.Column("zone_match_method", sa.String(length=50), nullable=True))
    op.add_column("policies", sa.Column("zone_num", sa.Integer(), nullable=True))
    op.add_column("policies", sa.Column("source_sheet", sa.String(length=20), nullable=True))


def downgrade() -> None:
    op.drop_column("policies", "source_sheet")
    op.drop_column("policies", "zone_num")
    op.drop_column("policies", "zone_match_method")
    op.drop_column("policies", "coordinate_source")
    op.drop_column("policies", "zone_source")
    op.drop_column("policies", "lon")
    op.drop_column("policies", "lat")
    op.drop_column("policies", "prime_rate")

    op.drop_index(op.f("ix_communes_zone_sismique"), table_name="communes")
    op.drop_index(op.f("ix_communes_wilaya_name"), table_name="communes")
    op.drop_index(op.f("ix_communes_wilaya_code"), table_name="communes")
    op.drop_index(op.f("ix_communes_commune_name"), table_name="communes")
    op.drop_index(op.f("ix_communes_code_commune"), table_name="communes")
    op.drop_table("communes")

