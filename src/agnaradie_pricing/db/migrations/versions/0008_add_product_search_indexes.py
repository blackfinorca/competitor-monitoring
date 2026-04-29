"""Add indexes for DB-only product search.

Revision ID: 0008_add_product_search_indexes
Revises: 0007_unify_matching
Create Date: 2026-04-29
"""

from alembic import op

revision = "0008_add_product_search_indexes"
down_revision = "0007_unify_matching"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_index("idx_cl_ean", "competitor_listings", ["ean"])
    op.create_index("idx_cl_competitor_sku", "competitor_listings", ["competitor_sku"])


def downgrade() -> None:
    op.drop_index("idx_cl_competitor_sku", table_name="competitor_listings")
    op.drop_index("idx_cl_ean", table_name="competitor_listings")
