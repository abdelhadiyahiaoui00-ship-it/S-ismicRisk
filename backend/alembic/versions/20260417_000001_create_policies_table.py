"""create policies table

Revision ID: 20260417_000001
Revises:
Create Date: 2026-04-17 00:00:01
"""
from __future__ import annotations

from alembic import op
import sqlalchemy as sa


revision = "20260417_000001"
down_revision = None
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "policies",
        sa.Column("id", sa.Integer(), autoincrement=True, nullable=False),
        sa.Column("source_row_number", sa.Integer(), nullable=False),
        sa.Column("policy_year", sa.Integer(), nullable=False),
        sa.Column("numero_police", sa.String(length=50), nullable=False),
        sa.Column("date_effet", sa.Date(), nullable=False),
        sa.Column("date_expiration", sa.Date(), nullable=False),
        sa.Column("type_risque", sa.String(length=120), nullable=False),
        sa.Column("code_wilaya", sa.String(length=2), nullable=False),
        sa.Column("zone_lookup_code_wilaya", sa.String(length=2), nullable=True),
        sa.Column("wilaya", sa.String(length=120), nullable=False),
        sa.Column("source_code_commune", sa.String(length=20), nullable=True),
        sa.Column("code_commune", sa.String(length=20), nullable=False),
        sa.Column("commune", sa.String(length=120), nullable=False),
        sa.Column("zone_sismique", sa.String(length=4), nullable=False),
        sa.Column("capital_assure", sa.Numeric(precision=18, scale=2), nullable=False),
        sa.Column("prime_nette", sa.Numeric(precision=18, scale=3), nullable=False),
        sa.Column("zone_policy_count_year", sa.Integer(), nullable=True),
        sa.Column("zone_capital_assure_total_year", sa.Numeric(precision=18, scale=2), nullable=True),
        sa.Column("wilaya_policy_count_year", sa.Integer(), nullable=True),
        sa.Column("wilaya_capital_assure_total_year", sa.Numeric(precision=18, scale=2), nullable=True),
        sa.Column("wilaya_zone_policy_count_year", sa.Integer(), nullable=True),
        sa.Column("wilaya_zone_capital_assure_total_year", sa.Numeric(precision=18, scale=2), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), server_default=sa.text("now()"), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), server_default=sa.text("now()"), nullable=False),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint("source_row_number"),
    )
    op.create_index(op.f("ix_policies_code_commune"), "policies", ["code_commune"], unique=False)
    op.create_index(op.f("ix_policies_code_wilaya"), "policies", ["code_wilaya"], unique=False)
    op.create_index(op.f("ix_policies_commune"), "policies", ["commune"], unique=False)
    op.create_index(op.f("ix_policies_numero_police"), "policies", ["numero_police"], unique=False)
    op.create_index(op.f("ix_policies_policy_year"), "policies", ["policy_year"], unique=False)
    op.create_index(op.f("ix_policies_source_row_number"), "policies", ["source_row_number"], unique=True)
    op.create_index(op.f("ix_policies_type_risque"), "policies", ["type_risque"], unique=False)
    op.create_index(op.f("ix_policies_wilaya"), "policies", ["wilaya"], unique=False)
    op.create_index(op.f("ix_policies_zone_sismique"), "policies", ["zone_sismique"], unique=False)


def downgrade() -> None:
    op.drop_index(op.f("ix_policies_zone_sismique"), table_name="policies")
    op.drop_index(op.f("ix_policies_wilaya"), table_name="policies")
    op.drop_index(op.f("ix_policies_type_risque"), table_name="policies")
    op.drop_index(op.f("ix_policies_source_row_number"), table_name="policies")
    op.drop_index(op.f("ix_policies_policy_year"), table_name="policies")
    op.drop_index(op.f("ix_policies_numero_police"), table_name="policies")
    op.drop_index(op.f("ix_policies_commune"), table_name="policies")
    op.drop_index(op.f("ix_policies_code_wilaya"), table_name="policies")
    op.drop_index(op.f("ix_policies_code_commune"), table_name="policies")
    op.drop_table("policies")
