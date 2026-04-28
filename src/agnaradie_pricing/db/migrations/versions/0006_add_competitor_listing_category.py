"""Add category to competitor listings.

Revision ID: 0006_add_competitor_listing_category
Revises: 0005_add_product_clusters
Create Date: 2026-04-28
"""

from alembic import op
import sqlalchemy as sa

revision = "0006_add_competitor_listing_category"
down_revision = "0005_add_product_clusters"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column("competitor_listings", sa.Column("category", sa.Text(), nullable=True))


def downgrade() -> None:
    op.drop_column("competitor_listings", "category")
